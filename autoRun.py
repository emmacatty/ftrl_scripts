import sys,os
import time
from multiprocessing import Process
from multiprocessing import Pool

SECOND_PER_DAY = 24 * 60 * 60

def exec_cmd(cmd):
    print "START %s" % cmd
    ret = os.system(cmd)
#time.sleep(3)
    if ret == 1:
        print "ERROR %s" % cmd
        return False
    return True

def offline_fea(i):
    cmd = 'sh run_phone.sh %s' % i
    return exec_cmd(cmd)

def download_data(dataPath, fileName):
    os.chdir("../data")
    targetDir = dataPath + fileName
    cmd = "hadoop --cluster c3prc-hadoop dfs -get " + targetDir
    ret = exec_cmd(cmd)
    os.chdir("../scripts")
    return ret

def model_filter(infile, outfile):
    lite_model = open(outfile, 'w')
    for line in open(infile, 'r'):
        if not line:
            break
        else:
            if line.split('\t')[1] != '0':
                lite_model.write(line)            
    lite_model.close()                                        

def predict_auc(filename, flag, curTS):
    cmd = "cat ../data/" + filename + "/* | gzip -d |"
    ds = time.strftime("%Y%m%d", time.localtime(curTS - 2*SECOND_PER_DAY))
    #model filter 0s
    model_filter("model."+flag+".2", "model."+flag+"."+ds)
    cmd += " ./ftrl_predict model." + flag + "." + ds + " 10 res." + flag +"."+ds
    if exec_cmd(cmd):
        cmd = "rm -rf ../data" + filename
        if exec_cmd(cmd):
            cmd = "cat res." + flag + "." + ds + " | ./AUC.py &> auc." + flag + "." + ds
            return exec_cmd(cmd)
    return False
    
def train_or_predict(round, filename, flag, daySpan, curTS):
    if round == 1:
        predict_auc(filename, flag, curTS)
    else:
        cmd = "cat ../data/" + filename + "/* | gzip -d | ./ftrl_train"
        if round < daySpan + 1:
            cmd += " -im model." + flag + "." + str(round+1)
        cmd += " -m model." + flag + "." + str(round) + " -l1 5 -core 10 -bias 1"
        if exec_cmd(cmd):
            #drop data
            cmd = "rm -rf ../data/" + filename
            return exec_cmd(cmd)
        return False
    
def train_core(daySpan, dataPath, flag, curTS):
    for i in range(daySpan + 1, 0, -1):
        ds = time.strftime("%Y%m%d", time.localtime(curTS - i * SECOND_PER_DAY))
        fileName = "date=%s_CPC_" % ds + flag
        if not download_data(dataPath, fileName):
            break
        if not train_or_predict(i, fileName, flag, daySpan):
            break

status = []
def err_callback(arg):
    if arg:
        status.append(0)
    else:
        status.append(1)
        
        
def train(daySpan, dataPath):
    curTS = time.time()
    # step1: offline fea extract
    # pool = Pool(min(24, daySpan + 1))
    # for i in range(1, daySpan + 2):
    #     pool.apply_async(func=offline_fea, args=(i, ), callback=err_callback)
    # pool.close()
    # pool.join()
    # pool.terminate()
    # if sum(status) != 0:
    #     print "offline fea extract faild: %s" % sum(status)
    #     return

    for i in range(1, daySpan + 2):
        offline_fea(i)
    
    # p_list=[]
    # for i in range(1, daySpan + 2):
    #     p = Process(target=offline_fea, args=(i, ))
    #     p.daemon = True
    #     p_list.append(p)
    # for p in p_list:
    #     p.start()
    # for p in p_list:
    #     p.join()

    # step2: download data & train & predict
    p_base = Process(target=train_core, args=(daySpan, dataPath, "base_feature", curTS, ))
    p_base.start()
    p_opt = Process(target=train_core, args=(daySpan, dataPath, "phone_video", curTS, ))
    p_opt.start()
    p_base.join()
    p_opt.join()

    # step3: auc

            
if __name__ == '__main__':
    dataPath = '/user/h_miui_ad/wwxu/exp_new/phone_context/'
    train(10, dataPath)
