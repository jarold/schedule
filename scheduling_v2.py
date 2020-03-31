# -*- coding: utf-8 -*-
"""
Created on Mon Mar 30 12:15:19 2020
version 0.21
@author: sohuavril
"""
'''
import sys
stdout_backup = sys.stdout
log_file = open("log.log","w")
sys.stdout = log_file
'''

import math
import os
import pandas as pd 
import numpy as np
import glob
import sqlite3
import queue
import xlrd
from datetime import datetime
from datetime import timedelta
from sklearn.linear_model import LinearRegression
import numpy as np
import pandas as pd





def clear_file(filename):
    '''用于文件初始化查找并清理冲突文件'''
    if os.path.exists(filename):
        os.remove(filename)
    return 1






def get_day():
    '''获取数据集的日期队列'''
    con = sqlite3.connect('user.db')
    sql = "select distinct createtime from data_table order by createtime "
    df_day = pd.read_sql(sql = sql ,con = con)
    con.close()
    return df_day

def get_startday():
    '''获取开始日期'''
    con = sqlite3.connect('user.db')
    cursor = con.cursor()
    sql = "select min(createtime) from data_table "
    start_day =  cursor.execute(sql).fetchone()
    con.close()
    return start_day

def get_endday():
    '''获取结束日期'''
    con = sqlite3.connect('user.db')
    cursor = con.cursor()
    sql = "select max(createtime) from data_table "
    end_day =  cursor.execute(sql).fetchone()
    con.close()
    return end_day

def calculate_today_demand(day):
    '''计算当天vm需求,添加入当天名称的sqlite表中'''
    today = day
    con = sqlite3.connect('user.db')
    cursor = con.cursor()
    sql = "select * from data_table join VMType on data_table.vmtype = VMType.supportVMType where createtime like '"+today+"'" 
    df_ctoday = pd.DataFrame(cursor.execute(sql).fetchall())
    df_ctoday.columns = ['index1','vmid','vmtype','createtime','releasetime','n','supporttype','status','cpu','memory','income']
    df_ctoday['n'] = 0
    df_ctoday['status'] = 'running'
    df_ctoday.to_sql(today,con,if_exists = 'append')
    con.close()
    #print("success calculate ",today," demand and write to db")
    return 1

def get_vmqueue (dataframe,queue):
    '''
    获取vmid队列
    '''
    if dataframe.empty ==False:
        for i in dataframe[2]:
            queue.put(i)
    return queue

def get_ncqueue (dataframe,queue):
    '''
    获取ncid队列
    '''
    if dataframe.empty ==False:
        for i in dataframe[1]:
            queue.put(i)
    return queue

def wt_db(vmid,ncid,today):
    '''
    分配执行
    '''
    con = sqlite3.connect('user.db')
    cursor = con.cursor()
    vcm = cursor.execute("select cpu,memory from "+"'"+today+"'"+" where vmid = '"+vmid+"'").fetchall()
    vcpu = int(vcm[0][0])
    vmemory = int(vcm[0][1])
    ncm = cursor.execute("select ncpu,nmemory,ucpu,umemory from nc_status where ncid = '"+ncid+"'").fetchall()
    ncpu = int(ncm[0][0])
    nmemory = int(ncm[0][1])
    ucpu =  int(ncm[0][2])
    umemory =  int(ncm[0][3])
    if (vcpu + ucpu <= ncpu) and (vmemory +umemory <= nmemory):
        ucpu = ucpu + vcpu
        umemory = umemory + vmemory
        cursor.execute("update nc_status set ucpu = ?,umemory = ? where ncid = "+"'"+ncid+"'",(ucpu,umemory))
        con.commit()
        cursor.execute("update "+"'"+today+"'"+" set n = "+"'"+ncid+"'"+" where vmid = "+"'"+vmid+"'" )
        con.commit()
        #print(vmid,"-------->",ncid,"vcpu:",vcpu,"vmemory:",vmemory,"ucpu:",ucpu,"umemory:",umemory)
        con.close()
        return 1
    else:
        #print("distribute unsuccess")
        con.close()
        return 0


def wt_db_cg(vmid,ncid,today):
    '''
    c系列和g系列的最佳配比分配执行
    '''
    con = sqlite3.connect('user.db')
    cursor = con.cursor()
    vcm = cursor.execute("select cpu,memory from "+"'"+today+"'"+" where vmid = '"+vmid+"'").fetchall()
    vcpu = int(vcm[0][0])
    vmemory = int(vcm[0][1])
    ncm = cursor.execute("select ncpu,nmemory,ucpu,umemory from nc_status where ncid = '"+ncid+"'").fetchall()
    ncpu = int(ncm[0][0])
    nmemory = int(ncm[0][1])
    ucpu =  int(ncm[0][2])
    umemory =  int(ncm[0][3])
    if (vcpu + ucpu <= ncpu) and (vmemory +umemory <= (nmemory/2)):
        ucpu = ucpu + vcpu
        umemory = umemory + vmemory
        cursor.execute("update nc_status set ucpu = ?,umemory = ? where ncid = "+"'"+ncid+"'",(ucpu,umemory))
        con.commit()
        cursor.execute("update "+"'"+today+"'"+" set n = "+"'"+ncid+"'"+" where vmid = "+"'"+vmid+"'" )
        con.commit()
        #print(vmid,"-------->",ncid,"vcpu:",vcpu,"vmemory:",vmemory,"ucpu:",ucpu,"umemory:",umemory)
        con.close()
        return 1
    else:
        #print("distribute unsuccess")
        con.close()
        return 0

def wt_db_cgx(vmid,ncid,today):
    '''
    c系列和g系列的最佳配比分配执行
    '''
    con = sqlite3.connect('user.db')
    cursor = con.cursor()
    vcm = cursor.execute("select cpu,memory from "+"'"+today+"'"+" where vmid = '"+vmid+"'").fetchall()
    vcpu = int(vcm[0][0])
    vmemory = int(vcm[0][1])
    ncm = cursor.execute("select ncpu,nmemory,ucpu,umemory from nc_status where ncid = '"+ncid+"'").fetchall()
    ncpu = int(ncm[0][0])
    nmemory = int(ncm[0][1])
    ucpu =  int(ncm[0][2])
    umemory =  int(ncm[0][3])
    if (vcpu + ucpu <= ncpu) and (vmemory +umemory <= nmemory):
        ucpu = ucpu + vcpu
        umemory = umemory + vmemory
        cursor.execute("update nc_status set ucpu = ?,umemory = ? where ncid = "+"'"+ncid+"'",(ucpu,umemory))
        con.commit()
        cursor.execute("update "+"'"+today+"'"+" set n = "+"'"+ncid+"'"+" where vmid = "+"'"+vmid+"'" )
        con.commit()
        #print(vmid,"-------->",ncid,"vcpu:",vcpu,"vmemory:",vmemory,"ucpu:",ucpu,"umemory:",umemory)
        con.close()
        return 1
    else:
        #print("distribute unsuccess")
        con.close()
        return 0
    
    
    
def day_10_before(today): 
    '''
    日期转换获得10天前日期
    '''
    t =datetime.strptime(today,"%Y-%m-%d")
    t_10 = t + timedelta(days = -10)
    return t_10.strftime("%Y-%m-%d")   

def distribute_c_r(ncq,vmq,today):
    '''
    单项分配c系列和r系列
    '''
    while (ncq.empty() == False) and (vmq.empty() == False):
        ncid = ncq.get()
        while (vmq.empty() == False):
            vmid = vmq.get()
            s = wt_db(vmid,ncid,today) 
            if s == 0:
                break
    #print("distribute ")

def init_firstday(today):
    '''第一天特殊处理'''
    con = sqlite3.connect('user.db')
    cursor = con.cursor()
    gm_d = cursor.execute("select sum(memory) from '"+today+"' where supporttype = 'ecs.g1'").fetchone()
    cm_d = cursor.execute("select sum(memory) from '"+today+"' where supporttype = 'ecs.c1'").fetchone() 
    rm_d = cursor.execute("select sum(memory) from '"+today+"' where supporttype = 'ecs.r1'").fetchone()
    n8_d = 0
    n4_d = 0
    n2_d = 0
     
    if rm_d[0] != None :
        temp = int(rm_d[0])
        while temp > 256 :
           n8_d = n8_d + 1
           temp = temp - 516
           
    if cm_d[0] != None and gm_d[0] != None :
        tempc = int(cm_d[0])
        tempg = int(gm_d[0])
        if tempc > tempg:
            n2_d = (tempc-tempg)/128 + 1
            n2_d = round(n2_d)
            
    if gm_d[0] != None :
        if tempc > tempg:
            n4_d = tempg/128
            n4_d = round(n4_d)
        else :
            n4_d = tempg/256
            n4_d = round(n4_d)
            
    demand = [8*n2_d,8*n4_d,8*n8_d]
    con.close()
    return demand        



def insert_nc(types,day):
    '''将报备NC添加进nc_status表中'''
    con = sqlite3.connect('user.db')
    cursor = con.cursor()
    temp = cursor.execute("select count(*) from nc_status").fetchone()
    temp_id = int(temp[0])
    nc_id = "nc_"+str(temp_id+1)
    
    if types == 'n2':
        machineType = 'NT-1-2'
        ncpu = 64
        nmemory = 128
    elif types == 'n4':
        machineType = 'NT-1-4'
        ncpu = 96
        nmemory = 256
    else:
        machineType = 'NT-1-8'
        ncpu = 104
        nmemory = 516
    cursor.execute("insert into nc_status values (?,?,?,?,?,'init',0,0,?)",(temp_id,nc_id,machineType,ncpu,nmemory,day))
    con.commit()
    con.close()
    #print("insert_nc----"+types+"-----success" )
    return 1
    

def predict_insert(predict_list,day):
    '''
    将预测新增结果加入报备的NC_status表中
    '''
    today = day
    predict = predict_list
    while predict[0]:
        insert_nc('n2',today)
        predict[0] = predict[0]-1
    while predict[1]:
        insert_nc('n4',today)
        predict[1] = predict[1]-1
    while predict[2]:
        insert_nc('n8',today)
        predict[2] = predict[2]-1
    
    #print("insert all predict_list")
    return 1


def init_nc():
    '''
    初始化NC已使用的cpu和memory
    '''
    con = sqlite3.connect('user.db')
    cursor = con.cursor()
    cursor.execute("update nc_status set ucpu = 0,umemory = 0 ")
    con.commit()
    con.close()
    
def update_nc(today):
    '''
    报备设备到期更新状态投产使用
    ''' 
    con = sqlite3.connect('user.db')
    day = day_10_before(today)
    cursor = con.cursor()
    ud_nc = pd.DataFrame(cursor.execute("select ncid from nc_status where creatime like "+"'"+day+"'").fetchall())                    
    cursor.execute("update nc_status set status = 'free' where creatime like "+"'"+day+"'" )    
    '''
    for i in ud_nc:
        print(i,"-----update statues from init to free")
    '''
    con.commit()
    con.close()

def distribute_c_g_r(qc,qg,qr,n4,today):
    '''
    单项分配c,g,r给n4
    '''   
    #global gncid
    gncid = 0  
    
    
    while (n4.empty() == False) and (qc.empty() == False) and (qg.empty() == False):
        gncid = n4.get()
        while (qc.empty() == False):
            vmcid = qc.get()
            flag = wt_db_cg(vmcid,gncid,today) 
            if flag == 0:
                #print("half",gncid,"is full for vmc")
                break
        while (qg.empty() == False):
            vmgid = qg.get()
            flag = wt_db_cgx(vmgid,gncid,today)
            if flag == 0:
                #print("half",gncid,"is full for vmg")
                break
    
    if gncid == 0 and n4.empty() == False:
        gncid = n4.get()
        
    flag = 1  
        
    while  (n4.empty() == False) and (qc.empty() == False):
        if flag == 1:
            vmcid = qc.get()
        flag = wt_db(vmcid,gncid,today)
        if flag == 0:
            gncid = n4.get()
    
    while  (n4.empty() == False) and (qg.empty() == False):
        if flag == 1:
            vmgid = qg.get()
        flag = wt_db(vmgid,gncid,today)
        if flag == 0:
            gncid = n4.get()
                
            
    while  (n4.empty() == False) and (qr.empty() == False):
        if flag == 1:
            vmrid = qr.get()
        flag = wt_db(vmrid,gncid,today)
        if flag == 0:
            gncid = n4.get()
    
    if n4.empty() == False:
        #print("all distribute")
        return 1
    else :
        #print("NC unavailible")
        return 0
    
def next_day(today):
    '''
    日期转换第二天
    '''
    t =datetime.strptime(today,"%Y-%m-%d")
    t_1 = t + timedelta(days = 1)
    return t_1.strftime("%Y-%m-%d")


def wtdb_demand():
    '''
    将vm需求写入sqlite的predict_data
    '''
    con = sqlite3.connect('user.db')
    cursor = con.cursor()
    ecs_c1 = cursor.execute("select sum(memory) from '"+today+"' where supporttype = 'ecs.c1'" ).fetchone()
    ecs_g1 = cursor.execute("select sum(memory) from '"+today+"' where supporttype = 'ecs.g1'" ).fetchone()
    ecs_r1 = cursor.execute("select sum(memory) from '"+today+"' where supporttype = 'ecs.r1'" ).fetchone()
    
    ecs_rc = cursor.execute("select sum(memory) from '"+today+"' where supporttype = 'ecs.c1' and releasetime = '"+today+"'").fetchone()
    ecs_rg = cursor.execute("select sum(memory) from '"+today+"' where supporttype = 'ecs.g1' and releasetime = '"+today+"'").fetchone()
    ecs_rr = cursor.execute("select sum(memory) from '"+today+"' where supporttype = 'ecs.r1' and releasetime = '"+today+"'").fetchone()
    
    lc = list(ecs_c1)
    lg = list(ecs_g1)
    lr = list(ecs_r1)
    
    rc = list(ecs_rc)
    rg = list(ecs_rg)
    rr = list(ecs_rr)
    
    if lc[0] == None:
        lc[0] = 0
    if lg[0] == None:
        lg[0] = 0
    if lr[0] == None:
        lr[0] = 0
        
    if rc[0] == None:
        rc[0] = 0
    if rg[0] == None:
        rg[0] = 0
    if rr[0] == None:
        rr[0] = 0 
        
    lc[0] = lc[0]- rc[0]
    lg[0] = lg[0]- rg[0]
    lr[0] = lr[0]- rr[0]
    
    cursor.execute("insert into predict_data values (?,?,?,?,?)",(count_day,today,lc[0],lg[0],lr[0]))
    con.commit()
    con.close()
    return 1


def predict_num():
    '''
    读取predict_data数据并进行规划预测
    '''
    con = sqlite3.connect('user.db')
    sql = "select * from predict_data"
    getdata = pd.read_sql(sql,con = con)
    
    x_data = getdata['index'][-10:]
    x_data = x_data.to_frame()
    temp = [count_day+1]
    p_data = pd.DataFrame(temp)
    
    c_data = getdata['ecs.c1'][-10:]
    model_c = LinearRegression()
    model_c.fit(x_data,c_data)
    LinearRegression(copy_X=True,fit_intercept=True,n_jobs=1,normalize=True)
    pr_c = model_c.predict(p_data)
    
    g_data = getdata['ecs.g1'][-10:]   
    model_g = LinearRegression()
    model_g.fit(x_data,g_data)
    LinearRegression(copy_X=True,fit_intercept=True,n_jobs=1,normalize=True)
    pr_g = model_g.predict(p_data)
    
    r_data = getdata['ecs.r1'][-10:]
    
    model_r = LinearRegression()
    model_r.fit(x_data,r_data)
    LinearRegression(copy_X=True,fit_intercept=True,n_jobs=1,normalize=True)
    pr_r = model_r.predict(p_data)
    
    temp_d = [pr_c[0],pr_g[0],pr_r[0]]
    return temp_d

def predict():
    '''
    将预测结果转换为新增NC数量 
    '''
    con = sqlite3.connect('user.db')
    cursor = con.cursor()
    c_day = count_day
    if c_day < 10:
        
        gm_d = cursor.execute("select sum(memory) from '"+startday[0]+"' where supporttype = 'ecs.g1'").fetchone()
        cm_d = cursor.execute("select sum(memory) from '"+startday[0]+"' where supporttype = 'ecs.c1'").fetchone() 
        rm_d = cursor.execute("select sum(memory) from '"+startday[0]+"' where supporttype = 'ecs.r1'").fetchone()
        n8_d = 0
        n4_d = 0
        n2_d = 0
         
        if rm_d[0] != None :
            temp = int(rm_d[0])
            while temp > 256 :
               n8_d = n8_d + 1
               temp = temp - 516
               
        if cm_d[0] != None and gm_d[0] != None :
            tempc = int(cm_d[0])
            tempg = int(gm_d[0])
            if tempc > tempg:
                n2_d = (tempc-tempg)/128 + 1
                n2_d = round(n2_d)
                
        if gm_d[0] != None :
            if tempc > tempg:
                n4_d = tempg/128
                n4_d = round(n4_d)
            else :
                n4_d = tempg/256
                n4_d = round(n4_d)
                
        demand = [n2_d,n4_d,n8_d]
        con.close()
        return demand
    
    else:
        pdata = predict_num()
        
        
        n2d = cursor.execute("SELECT sum(nmemory) from nc_status where machineType = 'NT-1-2'").fetchone()
        n4d = cursor.execute("SELECT sum(nmemory) from nc_status where machineType = 'NT-1-4'").fetchone()
        n8d = cursor.execute("SELECT sum(nmemory) from nc_status where machineType = 'NT-1-8'").fetchone()
        
        ln2d = list(n2d)
        ln4d = list(n4d)
        ln8d = list(n8d)
        
        if ln2d[0] == None:
            ln2d[0] = 0
        if ln4d[0] == None:
            ln4d[0] = 0
        if ln8d[0] == None:
            ln8d[0] = 0
        '''   
        if 5*(pdata[0]-pdata[1])-ln2d[0] >0:
            a = math.ceil((5*(pdata[0]-pdata[1])-ln2d[0])/128)
        else:
            a = 0
        
        if pdata[1]+pdata[0] > ln4d[0]:
            b = math.ceil(((pdata[1]+pdata[0] - ln4d[0])/256 )/9)
        else:
            b = 0
            
        if pdata[2] > 256 and pdata[2] > ln8d[0]:
            c = round(((pdata[2]-ln8d[0])/516)/9)           
        else:
            c = 0
        demand = [a,b,c]
        '''
        
        if pdata[0]>pdata[1]:
            a = math.ceil((pdata[0]-pdata[1])/128)
            b = int(math.sqrt(pdata[1]/256))
        else:
            a = 0
            b = int(math.sqrt(pdata[1]/256))
            
        if pdata[2] - 0.7*ln8d[0] > 256:        
            c = int((pdata[2]-0.7*ln8d[0])//516)
        else:
            c = 0
        demand = [a,b,c]
        con.close
        return demand
    
'''--------------------------------------------'''
'''----------------主程序开始-------------------'''    
'''--------------------------------------------'''   
    
'''---初始化变量---'''
total_cost = 0
total_income = 0
income = []
lost = []

'''---初始化队列---'''
q_g = queue.Queue()
q_c = queue.Queue()
q_r = queue.Queue()
q_n2 = queue.Queue()
q_n4 = queue.Queue() 
q_n8 = queue.Queue()

'''---清理影响文件---'''
clear_file('nc.csv')
clear_file('new_nc.csv')
clear_file('vm.csv')
clear_file('user.db')


'''---读取csv文件进入sqlite,数据库名user.db,表为data_table---'''
file_names = glob.glob("*.csv") 
con = sqlite3.connect('user.db')
cursor = con.cursor()
for file_name in file_names:
    df = pd.read_csv(file_name)
    df.to_sql('data_table',con=con,if_exists = 'append')
    #print('success read ',file_name,'csv to sqlite')



'''---读入VMType.xls,nc_Status.xls，predict_data.xls创建数据库表---'''

df = pd.read_excel('VMType.xls')
df.to_sql('VMType',con=con,if_exists = 'replace')



df = pd.read_excel('nc_Status.xls')
df.to_sql('nc_status',con=con,if_exists = 'replace')


df = pd.read_excel('predict_data.xls')
df.to_sql('predict_data',con=con,if_exists = 'replace')


con.close()

'''---完成数据库创建和初始化---'''   
 

   

'''----------获取日期----------'''    
day = get_day()
startday = get_startday()
endday = get_endday()
count_day = 0



'''-----进入日期主循环--------'''

try:

    for i in day['createtime']:
        print("--------------------------------")
        print("today is ",i)
        print("run time now:",datetime.now())
        count_day = count_day + 1
        
        con = sqlite3.connect('user.db')
        cursor = con.cursor()
        
        
        '''------清空队列-----'''
        q_g.queue.clear()
        q_c.queue.clear()
        q_r.queue.clear()
        q_n2.queue.clear()
        q_n4.queue.clear() 
        q_n8.queue.clear()
        
       
        today = i 
        
        '''---计算当天新增需求,加入日期表中---'''
        calculate_today_demand(today) 
        
        '''---对第一天进行初始化---'''
        
        if today == startday[0]:
            
            '''---计算第一天需求---'''
            demand = init_firstday(today)
            
            '''---将NC资源报备入库---'''
            predict_insert(demand,today)
            cursor.execute("update nc_status set status = 'free' ")
            con.commit()
            
            
        '''---对NC资源进行重新分配前初始化---'''
        init_nc()
        
        
        #day_before = day_10_before(today)
        
        '''---对10天前报备修改状态---'''
        update_nc(today)
        
        '''---将vm需求写入sqlite的predict_data---'''
        wtdb_demand()  
        
        '''---获取需求队列和资源队列---'''
        g_group = cursor.execute("select * from " +"'"+today+"'"+" where supporttype = 'ecs.g1' order by memory DESC").fetchall()
        c_group = cursor.execute("select * from " +"'"+today+"'"+" where supporttype = 'ecs.c1' order by memory DESC").fetchall()
        r_group = cursor.execute("select * from " +"'"+today+"'"+" where supporttype = 'ecs.r1' order by memory DESC").fetchall()
        #print(g_group)
        
        df_ggroup = pd.DataFrame(g_group)
        df_cgroup = pd.DataFrame(c_group)
        df_rgroup = pd.DataFrame(r_group)
        
        '''---获取vmid队列---'''
        get_vmqueue(df_cgroup,q_c).queue
        get_vmqueue(df_ggroup,q_g).queue
        get_vmqueue(df_rgroup,q_r).queue
        
        
        '''---get ncid queue---'''
        
        n2_group = cursor.execute("select * from nc_status where status = 'free' and machineType = 'NT-1-2'  ").fetchall()
        n4_group = cursor.execute("select * from nc_status where status = 'free' and machineType = 'NT-1-4'  ").fetchall()
        n8_group = cursor.execute("select * from nc_status where status = 'free' and machineType = 'NT-1-8'  ").fetchall()
        
        df_n2group = pd.DataFrame(n2_group)
        df_n4group = pd.DataFrame(n4_group)
        df_n8group = pd.DataFrame(n8_group)
        
        get_ncqueue(df_n2group,q_n2).queue
        get_ncqueue(df_n4group,q_n4).queue
        get_ncqueue(df_n8group,q_n8).queue
        
        '''---------完成队列获取----------'''
        
    
        '''----------开始分配资源---------'''    
        
        distribute_c_r(q_n8,q_r,today)
        distribute_c_r(q_n2,q_c,today)
        flag = distribute_c_g_r(q_c,q_g,q_r,q_n4,today) 
        
        '''---------计算收益-----------'''
        income_today = cursor.execute("select 24*sum(income) from '"+today+"' where n != 0 ").fetchone()
        income.append(income_today[0])
        
        '''--------释放资源-----------'''
        cursor.execute("update '"+today+"' set status = 'release' where releasetime like '"+today+"'")
        con.commit()
        
        
        '''---------predict---------'''
    
        tp = predict()
        print("tp:",tp)
    
        
        '''---------插入新增nc------'''
        predict_insert(tp,today)
    
        
    
        '''-------计算成本---------'''
        n2_price = cursor.execute("select count(*) from nc_status where creatime = '"+today+"' and machineType = 'NT-1-2'").fetchone()   
        n4_price = cursor.execute("select count(*) from nc_status where creatime = '"+today+"' and machineType = 'NT-1-4'").fetchone() 
        n8_price = cursor.execute("select count(*) from nc_status where creatime = '"+today+"' and machineType = 'NT-1-8'").fetchone()
        
        if n2_price[0] == None:
            n2_price[0] = 0
        if n4_price[0] == None:
            n2_price[0] = 0
        if n8_price[0] == None:
            n2_price[0] = 0
              
        nc_price = int(n2_price[0])*20016 + int(n4_price[0])*23516 + int(n8_price[0])*30016 
        
        print ("nc_price:",nc_price)
        
        free_cost = cursor.execute("select 24*sum(ncpu) from nc_status where  status = 'free' ").fetchone()
        
        if free_cost[0] == None:
            free_cost[0] = 0
        
        if flag == 0:
            lost_today = cursor.execute("select 24*sum(cpu) from '"+today+"' where n = 0 ").fetchone()
            if lost_today[0] == None:
                lost_today[0] = 0
            lost.append(lost_today[0])
            cost = int(lost_today[0]) + nc_price + float(free_cost[0]*3.6)
        else:
            lost.append(0)
            cost = nc_price + float(free_cost[0])*3.6
        
        
        '''-------当天结算---------'''
        total_cost += cost
        total_income += income_today[0]
        print("free_cost:",free_cost)
        print("******************************")
        print("结算工作")
        print("cost:",cost)
        print("income:",income_today[0])
        print("total cost:",total_cost)
        print("total income:",total_income)
        print("总收益率：",((total_income - total_cost)/total_cost)*100,"%")
        print("******************************")
        
        '''-----结果添加进数据库的表格中-------'''
    
        df_vm = pd.DataFrame(cursor.execute("select vmid,status,n,vmtype,cpu,memory,createtime,releasetime from '"+today+"'").fetchall()) 
        df_vm.columns = ["a","b","c","d","e","f","g","h"]
        df_vm = df_vm.reindex(columns = list('iabcdefgh'),fill_value = today)
        df_vm.columns = ["outputDate","vmId","status","ncId","vmType","cpu","memory","createTime","releaseTime"]
        df_vm.to_sql('vm',con,if_exists = 'append') 
        
        df_nc = pd.DataFrame(cursor.execute("select ncid,status,ncpu,nmemory,machineType,ucpu,umemory,creatime from nc_status where status = 'free'").fetchall()) 
        df_nc.columns = ["a","b","c","d","e","f","g","h"]
        df_nc = df_nc.reindex(columns = list('iabcdefgh'),fill_value = today)
        df_nc.columns = ["outputDate","ncId","status","totalCpu","totalMemory","machineType","usedCpu","usedMemory","createTime"]
        df_nc.to_sql('nc',con,if_exists = 'append')
        
        df_newnc = pd.DataFrame(cursor.execute("select ncid,status,ncpu,nmemory,machineType,ucpu,umemory,creatime from nc_status where creatime = '"+today+"'").fetchall()) 
        if df_newnc.empty != True:
            df_newnc.columns = ["a","b","c","d","e","f","g","h"]
            df_newnc = df_newnc.reindex(columns = list('iabcdefgh'),fill_value = today)
            df_newnc.columns = ["outputDate","ncId","status","totalCpu","totalMemory","machineType","usedCpu","usedMemory","createTime"]
            df_newnc.to_sql('newnc',con,if_exists = 'append')    
        
    
        '''----------将vm需求加入后一天需求表中--------'''
        tomorrow = next_day(today)
        df_vm_tomorrow =  pd.DataFrame(cursor.execute("select * from '"+today+"' where status = 'running'").fetchall())
        df_vm_tomorrow.columns = ["a","b","c","d","e","f","g","h","i","j","k","l"]
        df_vm_tomorrow = df_vm_tomorrow.drop('a',axis = 1)
        df_vm_tomorrow.columns = ['index1','vmid','vmtype','createtime','releasetime','n','supporttype','status','cpu','memory','income']
        df_vm_tomorrow.to_sql(tomorrow,con,if_exists = 'append')  
        con.close()
        
        '''------前一天结束，进入后一天循环-----'''
        
        
finally:
    
    '''------最终输出csv-----'''
    con = sqlite3.connect('user.db')
    
    df = pd.read_sql("select * from vm",con)
    df.to_csv('vm.csv')
    
    df = pd.read_sql("select * from nc",con)
    df.to_csv('nc.csv')
    
    df = pd.read_sql("select * from newnc",con)
    df.to_csv('new_nc.csv') 
    
    con.close()
    






























































    



    















