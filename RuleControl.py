# !/usr/bin/python
# -*- coding: utf-8 -*-

import commands
import datetime
import os
import random
import socket
import struct
import time
import uuid

import mysql.connector
from IPy import IP

from BCP.Common.Env.ConfigParameter import ConfigParameter
from BCP.Common.Env.LogExceptionHelp import LogExceptionHelp

router_bridge = "br1"  # 路由服务使用的bridge
compute_bridge = 'br0'  # 计算节点使用的bridge
rule_tableName = 'bcp_rule'  # 数据库表名
rule_normal = 0  # 默认，正常的DHCP场景
rule_dhcp = None  # 需要指定，多vlan的DHCP的场景
topo_example_table = 'bcp_topo_example'  # 拓扑实例表
db_flow_table = "bcp_flowtable"  # 大赛使用的流表对应关系


# 数据库连接
def sql_conn():
    return mysql.connector.connect(host=ConfigParameter.DBHost, user=ConfigParameter.DBUser,
                                   password=ConfigParameter.DBPasswd, database=ConfigParameter.DBName)


# 数据库插入函数
def sql_insert(SQL_command):
    try:
        dbconn = sql_conn()
        cursor = dbconn.cursor()
        cursor.execute(SQL_command)
        dbconn.commit()
        cursor.close()
        dbconn.close()
    except mysql.connector.Error as e:
        print('mysql.connector.Error:{}'.format(e))
        LogExceptionHelp.logException('add rule to database error: %s' % e)
    finally:
        if cursor:
            cursor.close()
        dbconn.close()


# 删除某大赛使用的leases文件
def remove_leases_file(context_uid):
    dhcp_leases_file = '/tmp/%s.leases' % context_uid
    try:
        os.remove(dhcp_leases_file)
    except:
        return


# 初始化每个大赛使用的流表，每次启动拓扑时调用
def init_flow_table(competition_id):
    dbconn = sql_conn()
    cursor = dbconn.cursor()
    # 检查大赛和流表号是否已经在数据库，如果流量号存在则重新随机生成，如果大赛存在则不做任何事
    while 1:
        flow_table = random.randint(1, 255)
        check_CMD = "select flow_table from %s WHERE flow_table = %d;" % (db_flow_table, flow_table)
        cursor.execute(check_CMD)
        result = cursor.fetchall()
        if not result:
            break

    try:
        insert_CMD = "insert into %s (uid,competition_id,flow_table) VALUES ('%s','%s',%d);" % (
            db_flow_table, str(uuid.uuid1()), competition_id, flow_table)
        cursor.execute(insert_CMD)
        dbconn.commit()
        ovs_cmd = 'ovs-ofctl add-flow %s priority=1,table=%d,actions=normal' % (router_bridge, flow_table)
        commands.getstatusoutput(ovs_cmd)
    except Exception, e:
        print str(e)
        LogExceptionHelp.logException("generate competition %s 's flow table failure: %s" % (competition_id, str(e)))
    finally:
        cursor.close()
        dbconn.close()


# 查找对应大赛的流表号及规则
def search_flow_table(competition_id):
    dbconn = sql_conn()
    cursor = dbconn.cursor()
    search_CMD = "select flow_table from %s WHERE competition_id = '%s';" % (db_flow_table, competition_id)
    try:
        cursor.execute(search_CMD)
        result = cursor.fetchall()
        flow_table = result[0][0]
    except Exception, e:
        print e
    finally:
        cursor.close()
        dbconn.close()
        return flow_table


# 大赛退出时清除使用的流表对应关系
def del_flow_table(competition_id):
    Flow_table = search_flow_table(competition_id)
    if Flow_table:
        dbconn = sql_conn()
        cursor = dbconn.cursor()
        del_sql_CMD = "delete from %s WHERE competition_id = '%s';" % (db_flow_table, competition_id)
        try:
            cursor.execute(del_sql_CMD)
            dbconn.commit()
            # 阶段3或阶段4时删除流表规则
            ovs_cmd = 'ovs-ofctl del-flows %s table=%d' % (router_bridge, Flow_table)
            print("****exec command :%s" % ovs_cmd)
            commands.getstatusoutput(ovs_cmd)
        except Exception, e:
            print str(e)
        finally:
            cursor.close()
            dbconn.close()
    else:
        print "can not find flow table.at RuleControl.py line 99"
        return False


# 根据虚拟机名获取虚拟机的vnet及MAC地址
def getOVSPort_MAC(VMName):
    Command_output = commands.getstatusoutput('virsh domiflist "%s"' % VMName)
    result = []
    if Command_output[0] == 0:
        for n in Command_output[1].split('\n'):
            if n.startswith('vnet'):
                MAC = n.split()[-1]
                Nic_name = n.split()[0]
                result.append([Nic_name, MAC])
        return result
    else:
        print "get vmInfo error"
    return False


# 计算网关、DHCP起始地址、租约过期时间函数
def net_dhcp_start_end_gw(network, Mask, step_flag=None, dhcp_lease_day=30):
    # 根据掩码计算出传入的IP或网络号计算出网络号
    Network = str(IP(network).make_net(Mask)).split('/')[0]
    # 根据得到的掩码计算可用的主机数
    Availed_ip = 2 ** (32 - Mask)
    # 将得到的网络转换为整数
    dhcp_int = socket.ntohl(struct.unpack("I", socket.inet_aton(Network))[0])
    # DHCP网关地址,默认为.1
    dhcp_gateway = socket.inet_ntoa(struct.pack('I', socket.htonl(dhcp_int + 1)))
    # 计算出DHCP池的起始，1~5为保留使用
    dhcp_start = socket.inet_ntoa(struct.pack('I', socket.htonl(dhcp_int + 2)))
    # 计算出DHCP池的结束
    dhcp_end = socket.inet_ntoa(struct.pack('I', socket.htonl(dhcp_int + (Availed_ip - 2))))
    # DHCP接口IP地址，场景8是.1 其他场景从.2开始
    if step_flag == 'student':
        dhcp_ipaddr = socket.inet_ntoa(struct.pack('I', socket.htonl(dhcp_int + 1)))
    else:
        dhcp_ipaddr = socket.inet_ntoa(struct.pack('I', socket.htonl(dhcp_int + 2)))
    # dhcp租约过期时间(30天后过期)
    X_days_after = datetime.datetime.now() + datetime.timedelta(days=dhcp_lease_day)
    # 将30后的时间转换为时间戳
    dhcp_lease_time = int(time.mktime(X_days_after.timetuple()))
    result = {'dhcp_start': dhcp_start, 'dhcp_end': dhcp_end,
              'dhcp_ipaddr': dhcp_ipaddr, 'dhcp_gateway': dhcp_gateway,
              'dhcp_lease_time': dhcp_lease_time}
    return result


def rule_student(competition_id, step_flag, student_ip, vm_mac, host_ip, vlan_ID, vm_ipaddr):  # 针对每个学生进行策略创建(循环每个虚拟机多次)
    try:
        add_commands = [
            'ovs-ofctl add-flow %s dl_type=0x0800,nw_src=%s,nw_dst=%s,actions=mod_dl_src:%s,mod_dl_dst:%s,'
            'mod_vlan_vid:%d,output:%d' % (
                router_bridge, student_ip, vm_ipaddr, ConfigParameter.VMGatewayMAC, vm_mac, vlan_ID,
                ConfigParameter.OvsServer_up_port_ID)
        ]
        del_commands = ['ovs-ofctl --strict del-flows %s dl_type=0x0800,nw_src=%s,nw_dst=%s' % (
            router_bridge, student_ip, vm_ipaddr)
                        ]
    except Exception, e:
        print str(e)
        LogExceptionHelp.logException('competition %s scene %d, '
                                      'generate the cmd for adding rules of student fail: %s' %
                                      (competition_id, step_flag, e))
        return None
    # 添加规则入库
    command_type = 'student'
    action = 'add'
    add_command = ' && '.join(add_commands)
    SQL_command = "insert into %s (uid,commands,competition_id,step_flag,student_ip,host_ip,action,command_type)" \
                  " values ('%s',\"%s\",'%s',%d,'%s','%s','%s','%s');" % (
                      rule_tableName, str(uuid.uuid1()), add_command, competition_id, step_flag, student_ip, host_ip,
                      action,
                      command_type)
    sql_insert(SQL_command)

    # 删除规则入库
    action = 'del'
    del_command = ' && '.join(del_commands)
    SQL_command = "insert into %s (uid,commands,competition_id,step_flag,student_ip,host_ip,action,command_type)" \
                  " values ('%s',\"%s\",'%s',%d,'%s','%s','%s','%s');" % (
                      rule_tableName, str(uuid.uuid1()), del_command, competition_id, step_flag, student_ip, host_ip,
                      action,
                      command_type)
    sql_insert(SQL_command)


# 互攻阶段
def rule_scene_attack2(competition_id, step_flag, student_ip, vm_mac, host_ip, vlan_ID,
                       vm_ipaddr):  # 针对每个学生进行策略创建(循环每个虚拟机多次)
    # 互攻阶段时，不同大赛的相同阶段使用不同的流表
    flow_table = search_flow_table(competition_id)
    try:
        add_commands = ['ovs-ofctl add-flow %s \'dl_type=0x0800,nw_src=%s,actions=resubmit(,%d)\'' % (
            router_bridge, student_ip, flow_table),
                        'ovs-ofctl add-flow %s table=%d,dl_type=0x0800,nw_dst=%s,actions=mod_dl_src:%s,'
                        'mod_dl_dst:%s,mod_vlan_vid:%d,output:%d' % (
                            router_bridge, flow_table, vm_ipaddr, ConfigParameter.VMGatewayMAC, vm_mac, vlan_ID,
                            ConfigParameter.OvsServer_up_port_ID),
                        ]
        del_commands = ['ovs-ofctl --strict del-flows %s dl_type=0x0800,nw_src=%s' % (router_bridge, student_ip),
                        'ovs-ofctl --strict del-flows %s table=%d,dl_type=0x0800,nw_dst=%s' % (
                            router_bridge, flow_table, vm_ipaddr),
                        ]
    except Exception, e:
        print str(e)
        LogExceptionHelp.logException('competition %s scene %d, '
                                      'generate the cmd for adding rules of student fail: %s' %
                                      (competition_id, step_flag, e))
        return None
    # 添加规则入库
    command_type = 'student'
    action = 'add'
    add_command = ' && '.join(add_commands)
    SQL_command = "insert into %s (uid,commands,competition_id,step_flag,student_ip,host_ip,action,command_type)" \
                  " values ('%s',\"%s\",'%s',%d,'%s','%s','%s','%s');" % (
                      rule_tableName, str(uuid.uuid1()), add_command, competition_id, step_flag, student_ip, host_ip,
                      action,
                      command_type)
    sql_insert(SQL_command)

    # 删除规则入库
    action = 'del'
    del_command = ' && '.join(del_commands)
    SQL_command = "insert into %s (uid,commands,competition_id,step_flag,student_ip,host_ip,action,command_type)" \
                  " values ('%s',\"%s\",'%s',%d,'%s','%s','%s','%s');" % (
                      rule_tableName, str(uuid.uuid1()), del_command, competition_id, step_flag, student_ip, host_ip,
                      action,
                      command_type)
    sql_insert(SQL_command)


def rule_vm(competition_id, step_flag, vm_ipaddr, host_ip, vlan_ID, net_wrok, net_mask):  # 针对每个虚拟机进行策略创建(循环每个虚拟机一次)
    vm_arp_reply(compute_bridge, Vlan_ID=vlan_ID, Network=net_wrok, Mask=net_mask, host_IP=host_ip, vm_ipaddr=vm_ipaddr,
                 competition_id=competition_id, step_flag=step_flag, command_scene=None, commands_type='init')


def rule_compute(competition_id, step_flag, host_ip, vm_interface_name, new_vlan_ID,
                 vmname):  # 场景3的时候调用，循环每个虚拟机,阶段ID填4，host_ip填计算节点的IP，new_vlan_ID传新vlan
    try:
        compute_add_commands = ['ovs-vsctl set port %s tag=%d' % (vm_interface_name, new_vlan_ID),
                                r"sed -i 's/<tag id='.*'\\/>/<tag id='\\''%d'\\''\\/>/g' /etc/libvirt/qemu/%s.xml" % (
                                    new_vlan_ID, vmname)
                                ]
    except Exception, e:
        print str(e)
        LogExceptionHelp.logException('competition %s scene %d, '
                                      'generate the cmd of compute node fail: %s' %
                                      (competition_id, step_flag, e))
    command_type = 'compute'
    action = 'add'
    add_command = ' && '.join(compute_add_commands)
    SQL_command = "insert into %s (uid,commands,competition_id,step_flag,host_ip,action,command_type) " \
                  "values ('%s',\"%s\",'%s',%d,'%s','%s','%s');" % (
                      rule_tableName, str(uuid.uuid1()), add_command, competition_id, step_flag, host_ip, action,
                      command_type)
    sql_insert(SQL_command)


# scp母本网卡设置静态IP地址
# vlan_ID
# mod_card                     要修改的是哪块网卡list[1,2]
# vm_ip_addr                   虚拟机IP地址
# vm_ipaddr_Mask               虚拟机掩码
# dhcp_listening_ipadd         DHCP监听地址
# dhcp_gateway_ipadd           DHCP网关地址
# VMName                       虚拟机名
# action                       添加/删除网卡IP
# －－－action == add，所有参数都必须传
# －－－action == mod，不传vlan
# －－－acton == del，传VMName，vlan_ID
def rule_scp_parent_ip(VMName, action, vlan_ID=None, mod_card=None, vm_ip_addr=None, vm_ipaddr_Mask=None,
                       dhcp_listening_ipadd=None, dhcp_gateway_ipadd=None):
    # 得到虚拟机的所有vnet及mac
    if vm_ip_addr and vm_ipaddr_Mask:
        result = net_dhcp_start_end_gw(vm_ip_addr, vm_ipaddr_Mask)
    vm_cards = getOVSPort_MAC(VMName)  # 返回[['vnet0', '52:54:00:96:5a:d3'], ['vnet1', 'fa:fd:1c:e4:b4:6e']]
    if not vm_cards or not mod_card:
        return False
    vm_vnet = vm_cards[mod_card][0]
    vm_mac = vm_cards[mod_card][1]
    try:
        if action == '1' or action == 'add':
            CMD = ['ip netns add VLAN%d' % vlan_ID,
                   'ip link add vlan%d-out type veth peer name vlan%d-in' % (vlan_ID, vlan_ID),
                   'ip link set vlan%d-in netns VLAN%d' % (vlan_ID, vlan_ID),
                   'ovs-vsctl add-port %s vlan%d-out' % (compute_bridge, vlan_ID),
                   'ovs-vsctl set port vlan%d-out tag=%d' % (vlan_ID, vlan_ID),
                   'ip link set vlan%d-out up' % vlan_ID,
                   'ip netns exec VLAN%d ip link set vlan%d-in up' % (vlan_ID, vlan_ID),
                   'ip netns exec VLAN%d ip add add %s/%d dev vlan%d-in' % (
                       vlan_ID, result['dhcp_gateway'], vm_ipaddr_Mask, vlan_ID),
                   "echo '%d %s %s * 01:%s' > /tmp/%d.leases" % (0, vm_mac, vm_ip_addr, vm_mac, vlan_ID),
                   '[ -e /tmp/%d.pid ] && kill -9 `cat /tmp/%d.pid`' % (vlan_ID, vlan_ID),
                   'ip netns exec VLAN%d dnsmasq --strict-order --interface vlan%d-in --except-interface lo'
                   ' --bind-interfaces --dhcp-range=%s,%s,infinite --dhcp-option=3,%s --dhcp-no-override'
                   ' --dhcp-leasefile=/tmp/%d.leases --pid-file=/tmp/%d.pid' % (
                       vlan_ID, vlan_ID, vm_ip_addr, vm_ip_addr, result['dhcp_gateway'], vlan_ID, vlan_ID),
                   'virsh domif-setlink %s %s down' % (VMName, vm_vnet),
                   'sleep 1',
                   'virsh domif-setlink %s %s up' % (VMName, vm_vnet)
                   ]
        if action == '2' or action == 'mod':
            CMD = ['[ -e /tmp/%d.pid ] && kill -9 `cat /tmp/%d.pid`' % (vlan_ID, vlan_ID),
                   "echo '%d %s %s * 01:%s' > /tmp/%d.leases" % (0, vm_mac, vm_ip_addr, vm_mac, vlan_ID),
                   'ip netns exec VLAN%d dnsmasq --strict-order --interface vlan%d-in --except-interface lo'
                   ' --bind-interfaces --dhcp-range=%s,%s,infinite --dhcp-option=3,%s --dhcp-no-override'
                   ' --dhcp-leasefile=/tmp/%d.leases --pid-file=/tmp/%d.pid' % (
                       vlan_ID, vlan_ID, vm_ip_addr, vm_ip_addr, result['dhcp_gateway'], vlan_ID, vlan_ID),
                   'virsh domif-setlink %s %s down' % (VMName, vm_vnet),
                   'sleep 1',
                   'virsh domif-setlink %s %s up' % (VMName, vm_vnet)
                   ]
        if action == '3' or action == 'del':
            CMD = ['ip netns del VLAN%d' % vlan_ID,
                   'ip link del vlan%d-out type veth peer name vlan%d-in' % (vlan_ID, vlan_ID),
                   'ovs-vsctl del-port %s vlan%d-out' % (compute_bridge, vlan_ID),
                   '[ -e /tmp/%d.pid ] && kill -9 `cat /tmp/%d.pid`' % (vlan_ID, vlan_ID),
                   '[ -e /tmp/%d.pid ] && rm -rf /tmp/%d.pid' % (vlan_ID, vlan_ID),
                   '[ -e /tmp/%d.leases ] && rm -rf /tmp/%d.leases' % (vlan_ID, vlan_ID)
                   ]
    except Exception, e:
        print str(e)
        LogExceptionHelp.logException("the cmd of generating dhcp for parent network is failure: %s" % e)
        return None
    Exec_command = ' ; '.join(CMD)
    if Exec_command:
        result = commands.getstatusoutput(Exec_command)
        if result[0] != 0:
            print("★----exec error---- %s - [result：%s]" % (Exec_command, result[1]))
            return False
        else:
            print "★----exec commands----- %s" % Exec_command
            return True
    else:
        print '-------no command can be exec------'
        return False


def Data_Mirror(compute_bridge, Mirror_out_port,
                host_ip, competition_id, step_flag):
    """
    数据镜像(数据镜像,每个计算节点需要执行的命令)
    (每个br0的往返流量全部镜像到某一端口，端口在配置文件中指定),
    添加镜像
    :param compute_bridge:
    :param Mirror_out_port:
    :param host_ip:
    :param competition_id:
    :param step_flag:
    :return:
    """
    try:
        Mirror_add_commands = [
            'ovs-vsctl -- add bridge %s mirrors @m -- '
            '--id=@m create mirror name=mymirror' % compute_bridge,
            'ovs-vsctl set mirror mymirror select_all=1',
            'ovs-vsctl set mirror mymirror output_vlan=4093'
        ]
    except Exception, e:
        print str(e)
        LogExceptionHelp.logException("generate the cmd for adding data mirror fail: %s" % e)
    Action = 'add'
    Commands = ' && '.join(Mirror_add_commands)
    SQL_command = "insert into %s (uid,commands,competition_id,step_flag,host_ip,action)" \
                  " values ('%s',\"%s\",'%s',%d,'%s','%s');" % (
                      rule_tableName, str(uuid.uuid1()), Commands, competition_id, step_flag, host_ip, Action)
    sql_insert(SQL_command)

    # 移除镜像(每个计算节点都需要执行)
    try:
        Mirror_del_commands = ['ovs-vsctl clear Bridge %s mirrors' % compute_bridge
                               ]
    except Exception, e:
        print str(e)
        LogExceptionHelp.logException("generate the cmd for deleting data mirror fail: %s" % e)
        return None
    Action = 'del'
    Commands = ' ; '.join(Mirror_del_commands)
    SQL_command = "insert into %s (uid,commands,competition_id,step_flag,host_ip,action) " \
                  "values ('%s',\"%s\",'%s',%d,'%s','%s');" % (
                      rule_tableName, str(uuid.uuid1()), Commands, competition_id, step_flag, host_ip, Action)
    sql_insert(SQL_command)


# 学生端DHCP
# 初始化(学生端DHCP,OVS服务器侧)
def client_dhcp_init(client_dhcp_network, client_Mask):
    leases_file = '/tmp/client_dhcp.leases'
    pid_file = '/tmp/client_dhcp.pid'
    last_gateway_file = '/tmp/client_last_gateway.txt'  # 保存DHCP上一次的网关信息
    command_scene = 1
    try:
        result = net_dhcp_start_end_gw(client_dhcp_network, client_Mask, 'student')
    except Exception, e:
        print str(e)
        LogExceptionHelp.logException("initialize adding dhcp to student's vm fail: %s" % e)
        return None
    client_dhcp_gw = result['dhcp_gateway']
    client_dhcp_start = result['dhcp_start']
    client_dhcp_end = result['dhcp_end']
    try:
        client_init_commands = ['ip link set %s up' % router_bridge,
                                '[ `ip add | grep %s | wc -l` == 0 ]  && ip add add %s/%d dev %s' % (
                                    client_dhcp_gw, client_dhcp_gw, client_Mask, router_bridge),
                                '[ -e %s ] && kill -9 `cat %s`' % (pid_file, pid_file),
                                "echo '%s/%d' > %s" % (client_dhcp_gw, client_Mask, last_gateway_file),
                                'dnsmasq --strict-order --listen-address %s --except-interface lo '
                                '--bind-interfaces --dhcp-range=%s,%s,infinite --dhcp-option=3,%s '
                                '--dhcp-no-override --dhcp-leasefile=%s --pid-file=%s' % (
                                    client_dhcp_gw, client_dhcp_start, client_dhcp_end,
                                    client_dhcp_gw, leases_file, pid_file)
                                ]
        client_del_commands = [
            '[ -e %s ] && [ $(ip add | grep `cat %s` | grep -v grep | wc -l) > 0 ]'
            ' && ip add del `cat %s` dev %s' % (
                last_gateway_file, last_gateway_file, last_gateway_file, router_bridge),
            '[ -e %s ] && kill -9 `cat %s`' % (pid_file, pid_file),
            '[ -e %s ] && rm -rf %s' % (pid_file, pid_file),
            '[ -e %s ] && rm -rf %s' % (leases_file, leases_file)
        ]
    except Exception, e:
        print str(e)
        LogExceptionHelp.logException("generate the cmd of adding dhcp to student's vm fail: %s" % e)
        return False
    # －－－执行命令（每次先删除，再添加）－－－
    # 删除命令
    del_CMD = [' ; '.join(client_del_commands)]
    exec_command(del_CMD)
    # 添加命令
    add_CMD = [' ; '.join(client_init_commands)]
    exec_command(add_CMD)


# 场景九(虚拟机DHCP)
# DHCP命令初始化并且入库
#     Uuid：           数据库uid，
#     compute_bridge：    计算节点网桥名称(固定值),
#     step_flag：      阶段flag，
#     competition_id： 大赛/课程uid，
#     vlan_ID：        VLAN，
#     host_ip：        执行命令的主机IP，
#     vm_dhcp_network：可分配的IP段，
#     vm_ipaddr_Mask： 分配的子网掩码（整数），
#     dhcp_ipadd：     DNSMsq进程监听IP，
#     vm_mac_address：具体虚拟机MAC，
#     vm_ipaddr：     具体虚拟机IP，
#     command_scene：   0普通或1DHCP，
#     dhcp_for：      vm虚拟机或student学生机，
#     Action：        add添加或del删除。
#     times：         场景3时只需要一个网关，用于区分
def vm_dhcp_init(compute_bridge, step_flag, competition_id, vlan_ID, host_ip, vm_dhcp_network, vm_ipaddr_Mask,
                 times=None, dhcp_listening_ipadd=None, vm_mac_address=None, vm_ip_addr=None, dhcp_gateway_ipadd=None):
    # 计算起始、结束ip、网关（dnsmasq命令必须加此参数，否则不分配）。
    result = net_dhcp_start_end_gw(vm_dhcp_network, vm_ipaddr_Mask, step_flag)
    dhcp_lease_time = result['dhcp_lease_time']
    # dhcp_start = vm_ip_addr if vm_ip_addr else result['dhcp_start']
    dhcp_start = result['dhcp_start']
    # dhcp_end = vm_ip_addr if vm_ip_addr else result['dhcp_end']
    dhcp_end = result['dhcp_end']
    dhcp_gateway = dhcp_gateway_ipadd if dhcp_gateway_ipadd else result['dhcp_gateway']
    # dhcp_listen_add = dhcp_listening_ipadd if dhcp_listening_ipadd else result['dhcp_ipaddr']
    dhcp_listen_add = result['dhcp_ipaddr']
    step4_dhcp_leases_file = '/tmp/%s.leases' % competition_id
    # 如果是场景3的话，保存所有虚拟机的leases信息
    if step_flag == ConfigParameter.scene_attack1:
        with open(step4_dhcp_leases_file, 'a+') as f:
            f.write('%d %s %s * 01:%s\n' % (dhcp_lease_time, vm_mac_address, vm_ip_addr, vm_mac_address))
    # DHCP添加命令
    try:
        # DHCP通用命令（所有场景通用）
        dhcp_init_commands = ['ip netns add VLAN%d' % vlan_ID,
                              'ip link add vlan%d-out type veth peer name vlan%d-in' % (vlan_ID, vlan_ID),
                              '[ -e /var/run/netns/VLAN%d ] && [ -e /sys/devices/virtual/net/vlan%d-in ]'
                              ' && [ -e /sys/devices/virtual/net/vlan%d-out ]' % (
                                  vlan_ID, vlan_ID, vlan_ID),
                              'ip link set vlan%d-in netns VLAN%d' % (vlan_ID, vlan_ID),
                              'ovs-vsctl add-port %s vlan%d-out' % (compute_bridge, vlan_ID),
                              'ovs-vsctl set port vlan%d-out tag=%d' % (vlan_ID, vlan_ID),
                              'ip link set vlan%d-out up' % vlan_ID,
                              'ip netns exec VLAN%d ip link set vlan%d-in up' % (vlan_ID, vlan_ID),
                              'ip netns exec VLAN%d ip add add %s/%d dev vlan%d-in' % (
                                  vlan_ID, dhcp_listen_add, vm_ipaddr_Mask, vlan_ID),
                              ]
        # 正常场景（非场景4）的dhcp命令
        normal_dhcp_commands = ["echo '%d %s %s * 01:%s' >> /tmp/%d.leases" % (
            dhcp_lease_time, vm_mac_address, vm_ip_addr, vm_mac_address, vlan_ID),
                                '[ -e /tmp/%d.pid ] && kill -9 `cat /tmp/%d.pid`' % (vlan_ID, vlan_ID),
                                'ip netns exec VLAN%d dnsmasq --strict-order --interface vlan%d-in'
                                ' --except-interface lo --bind-interfaces --dhcp-range=%s,%s,infinite'
                                ' --dhcp-option=3,%s --dhcp-no-override --dhcp-leasefile=/tmp/%d.leases'
                                ' --pid-file=/tmp/%d.pid' % (
                                    vlan_ID, vlan_ID, dhcp_start, dhcp_end, dhcp_gateway, vlan_ID, vlan_ID)
                                ]
        # 互攻阶段（场景4）的dhcp命令
        scene_attack2_dhcp_commands = [
            'ip netns exec VLAN%d dnsmasq --strict-order --interface vlan%d-in --except-interface lo'
            ' --bind-interfaces --dhcp-range=%s,%s,infinite --dhcp-option=3,%s --dhcp-no-override'
            ' --dhcp-leasefile=%s --pid-file=/tmp/%d.pid' % (
                vlan_ID, vlan_ID, dhcp_start, dhcp_end, dhcp_gateway, step4_dhcp_leases_file, vlan_ID)]
    except Exception, e:
        print str(e)
        LogExceptionHelp.logException("generate the cmd of making dhcp for scene %s fail: %s" % (step_flag, e))
        return None
    if step_flag == ConfigParameter.scene_attack2:
        [dhcp_init_commands.append(x) for x in scene_attack2_dhcp_commands]
    else:
        [dhcp_init_commands.append(x) for x in normal_dhcp_commands]
    # 入库
    print dhcp_init_commands
    Commands = ' ;  '.join(dhcp_init_commands)
    Action = 'add'
    command_scene = 1
    dhcp_for = 'vm'
    SQL_command = "insert into %s (uid,commands,competition_id,step_flag,host_ip,action,command_scene,dhcp_for)" \
                  " values ('%s',\"%s\",'%s',%d,'%s','%s',%d,'%s');" % (
                      rule_tableName, str(uuid.uuid1()), Commands, competition_id, step_flag, host_ip, Action,
                      command_scene,
                      dhcp_for)
    sql_insert(SQL_command)
    # 场景2时添加ARP reply规则
    if step_flag == ConfigParameter.scene_infiltrate:
        vm_arp_reply(compute_bridge=compute_bridge, Network=vm_dhcp_network, Mask=vm_ipaddr_Mask, host_IP=host_ip,
                     vm_ipaddr=vm_ip_addr, competition_id=competition_id, step_flag=step_flag,
                     command_scene=command_scene, commands_type=None)
    # 如果是场景3时，只需要入库一个网关
    if step_flag == ConfigParameter.scene_attack1 and times <= 0:
        return True
    # 网关IP地址的创建与清除
    try:
        dhcp_gateway_add = ['[ `ip add | grep %s | grep -v grep | wc -l` == 0 ] && ip add add %s/%d dev %s' % (
            dhcp_gateway, dhcp_gateway, vm_ipaddr_Mask, router_bridge)
                            ]
    except Exception, e:
        print str(e)
        LogExceptionHelp.logException("generate the cmd of setting gateway of dhcp "
                                      "for scene %s fail: %s" % (step_flag, e))
        return None
    Commands = ' ;  '.join(dhcp_gateway_add)
    SQL_command = "insert into %s (uid,commands,competition_id,step_flag,host_ip,action,command_scene,dhcp_for)" \
                  " values ('%s',\"%s\",'%s',%d,'%s','%s',%d,'%s');" % (
                      rule_tableName, str(uuid.uuid1()), Commands, competition_id, step_flag, ConfigParameter.RouteIP,
                      Action,
                      command_scene, dhcp_for)
    sql_insert(SQL_command)
    Action = 'del'
    try:
        dhcp_gateway_del = ['[ `ip add | grep %s | grep -v grep | wc -l` > 0 ] && ip add del %s/%d dev %s' % (
            dhcp_gateway, dhcp_gateway, vm_ipaddr_Mask, router_bridge)
                            ]
    except Exception, e:
        print str(e)
        LogExceptionHelp.logException("generate the cmd of deleting gateway of dhcp"
                                      " for scene %s fail: %s" % (step_flag, e))
        return None
    Commands = ' ;  '.join(dhcp_gateway_del)
    SQL_command = "insert into %s (uid,commands,competition_id,step_flag,host_ip,action,command_scene,dhcp_for)" \
                  " values ('%s',\"%s\",'%s',%d,'%s','%s',%d,'%s');" % (
                      rule_tableName, str(uuid.uuid1()), Commands, competition_id, step_flag, ConfigParameter.RouteIP,
                      Action,
                      command_scene, dhcp_for)
    sql_insert(SQL_command)


# DHCP拆除命令并入库
# Uuid               数据库uid
# compute_bridge        网桥名称
# vlan_ID            vlan tag
# competition_id     大赛/课程uid
# step_flag          阶段flag
# host_ip            执行命令的主机ip
# dhcp_for='vm'      vm虚拟机或student学生机，
# Action = 'del'     add添加或del删除。
# command_scene=1      0普通或1DHCP，
def vm_clear_dhcp(compute_bridge, vlan_ID, competition_id, step_flag, host_ip):
    # DHCP拆除命令
    step4_dhcp_leases_file = '/tmp/%s.leases' % competition_id
    try:
        clear_commands = ['ip netns del VLAN%d' % vlan_ID,
                          'ip link del vlan%d-out type veth peer name vlan%d-in' % (vlan_ID, vlan_ID),
                          'ovs-vsctl del-port %s vlan%d-out' % (compute_bridge, vlan_ID),
                          '[ -e /tmp/%d.pid ] && kill -9 `cat /tmp/%d.pid`' % (vlan_ID, vlan_ID),
                          '[ -e /tmp/%d.pid ] && rm -rf /tmp/%d.pid' % (vlan_ID, vlan_ID),
                          '[ -e /tmp/%d.leases ] && rm -rf /tmp/%d.leases' % (vlan_ID, vlan_ID)
                          ]
    except Exception, e:
        print str(e)
        LogExceptionHelp.logException("generate the cmd of clean up dhcp for scene %s fail: %s" % (step_flag, e))
        return None
    # 'ip add del %s/%d dev %s' % (dhcp_listening_ipadd, vm_ipaddr_Mask, compute_bridge)
    #  #词条不需要，接口移除后ip就自动消失了

    # 场景4与场景3的leases文件不同，所以拆除的时候也不相同
    if step_flag == ConfigParameter.scene_attack2:
        clear_commands += ['rm -rf %s' % step4_dhcp_leases_file
                           ]
    else:
        clear_commands += ['[ -e /tmp/%d.leases ] && rm -rf /tmp/%d.leases' % (vlan_ID, vlan_ID)
                           ]

    # 入库
    dhcp_for = 'vm'
    Action = 'del'
    command_scene = 1
    clear_command = ' ; '.join(clear_commands)
    SQL_command = "insert into %s (uid,commands,competition_id,step_flag,host_ip,action,command_scene,dhcp_for)" \
                  " values ('%s',\"%s\",'%s',%d,'%s','%s',%d,'%s');" % (
                      rule_tableName, str(uuid.uuid1()), clear_command, competition_id, step_flag, host_ip, Action,
                      command_scene,
                      dhcp_for)
    sql_insert(SQL_command)


# 虚拟机ARP网关reply包响应
def vm_arp_reply(compute_bridge=None, Vlan_ID=None, Network=None, Mask=None, host_IP=None, vm_ipaddr=None,
                 competition_id=None, step_flag=None, command_scene=None, commands_type=None):
    # 得到网关及IP地址的16进制字符串
    result = net_dhcp_start_end_gw(Network, Mask)
    gateway_mac_hex = '0x' + ConfigParameter.VMGatewayMAC.replace(':', '')
    gateway_ip_hex = IP(result['dhcp_gateway']).strHex()
    command_scene = command_scene if command_scene else rule_normal
    commands_type = commands_type if commands_type else 'None'
    try:
        add_commands = [
            "\"ovs-ofctl add-flow %s 'arp,arp_op=0x01,arp_spa=%s,arp_tpa=%s,"
            "dl_dst=ff:ff:ff:ff:ff:ff,actions=move:NXM_OF_ETH_SRC[]->NXM_OF_ETH_DST[],"
            "mod_dl_src:%s,load:0x2->NXM_OF_ARP_OP[],move:NXM_NX_ARP_SHA[]->NXM_NX_ARP_THA[],"
            "move:NXM_OF_ARP_SPA[]->NXM_OF_ARP_TPA[],load:%s#x->NXM_NX_ARP_SHA[],"
            "load:%s#x->NXM_OF_ARP_SPA[],in_port'\"" %
            (compute_bridge, vm_ipaddr, result['dhcp_gateway'],
             ConfigParameter.VMGatewayMAC, gateway_mac_hex, gateway_ip_hex)
        ]
        del_commands = [
            "\"ovs-ofctl --strict del-flows %s 'arp,arp_op=0x01,arp_spa=%s,"
            "arp_tpa=%s,dl_dst=ff:ff:ff:ff:ff:ff'\"" % (
                compute_bridge, vm_ipaddr, result['dhcp_gateway'])
        ]
    except Exception, e:
        print str(e)
        LogExceptionHelp.logException("generate the cmd of making the ARP active fail: %s" % e)
        return None
    # 添加命令入库：
    Action = 'add'
    add_CMD = ' && '.join(add_commands)
    SQL_command = "insert into %s (uid,commands,competition_id,step_flag,host_ip,`action`,command_scene,command_type)" \
                  " values ('%s',%s,'%s',%d,'%s','%s',%d,'%s');" % (
                      rule_tableName, str(uuid.uuid1()), add_CMD, competition_id, step_flag, host_IP, Action,
                      command_scene,
                      commands_type)
    sql_insert(SQL_command)
    # 删除命令入库：
    Action = 'del'
    del_CMD = ' && '.join(del_commands)
    SQL_command = "insert into %s (uid,commands,competition_id,step_flag,host_ip,`action`,command_scene,command_type)" \
                  " values ('%s',%s,'%s',%d,'%s','%s',%d,'%s');" % (
                      rule_tableName, str(uuid.uuid1()), del_CMD, competition_id, step_flag, host_IP, Action,
                      command_scene,
                      commands_type)
    sql_insert(SQL_command)


# 删除大赛、阶段相关的规则
def clear_rules(competition_id=None, step_flag=None, dhcp_for=None, command_scene=None, student_ip=None):
    SQL_Field = ''
    Argvs = {'competition_id': competition_id, 'step_flag': step_flag, 'dhcp_for': dhcp_for,
             'command_scene': command_scene, 'student_ip': student_ip}
    times = 0
    for k, v in Argvs.iteritems():
        if v == None:
            continue
        else:
            if times == 0:
                SQL_Field += "%s = '%s'" % (k, str(v))
                times += 1
            else:
                SQL_Field += " and %s = '%s'" % (k, str(v))
    SQL_command = "delete from %s where %s;" % (rule_tableName, SQL_Field)
    try:
        dbconn = sql_conn()
        cursor = dbconn.cursor()
        cursor.execute(SQL_command)
        dbconn.commit()
    except mysql.connector.Error as e:
        print('mysql.connector.Error:{}'.format(e))
    finally:
        if cursor:
            cursor.close()
        dbconn.close()


# 根据提供的相关参数查找信息，动态生成SQL语句，参数值不固定时
# student_ip                     学生机的IP地址
# competition_id                 大赛ID
# step_flag                      阶段标识(1,2,3,4)
# action                         命令动作(add/del)
# command_scene                    0-正常 1-DHCP
# dhcp_for                       vm虚拟机的DHCP   student学生机的DHCP
# command_type                   执行命令的类型：student-学生的IP，循环每个虚拟机执行的命令  vm-每个虚拟机执行一次的命令  init-初始化命令，每个拓扑实例执行一次的命令 compute-每个计算节点需要执行的命令
def search_commands(student_ip=None, competition_id=None, step_flag=None, action=None, dhcp_for=None,
                    command_scene=None, command_type=None):
    SQL_Field = ''
    if step_flag == ConfigParameter.scene_attack1 or step_flag == ConfigParameter.scene_attack2:
        command_scene = None
    Argvs = {'student_ip': student_ip, 'competition_id': competition_id, 'step_flag': step_flag, 'action': action,
             'dhcp_for': dhcp_for, 'command_scene': command_scene, 'command_type': command_type}
    times = 0
    for k, v in Argvs.iteritems():
        if v == None:
            continue
        else:
            if times == 0:
                SQL_Field += "%s = '%s'" % (k, str(v))
                times += 1
            else:
                SQL_Field += " and %s = '%s'" % (k, str(v))
    SQL_command = "select host_ip,commands from %s where %s;" % (rule_tableName, SQL_Field)

    # 连接数据库
    try:
        dbconn = sql_conn()
        cursor = dbconn.cursor()
        cursor.execute(SQL_command)
        # 得到host_ip和command
        host_commands = {}
        for (HOST, CMD) in cursor:
            if host_commands.has_key(HOST):
                host_commands[HOST].append(CMD)
            else:
                host_commands[HOST] = [CMD]
    except mysql.connector.Error as e:
        print('mysql.connector.Error:{}'.format(e))
        LogExceptionHelp.logException("query the cmd %s of scene %d "
                                      "from database fail: %s" % (step_flag, action, e))
    finally:
        if cursor:
            cursor.close()
        dbconn.close()
    return host_commands


# 命令执行函数
def exec_command(coms):
    if coms:
        for command in coms:
            result = commands.getstatusoutput(command)
            if result[0] != 0:
                print("★----exec error---- %s - [result：%s]" % (command, result[1]))
            else:
                print "★----exec commands----- %s" % command
        return True
    else:
        print "exec command Error"
        return None
