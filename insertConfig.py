import os
filenames = ["./pubsub/bigquery-controller.yaml","./pubsub/twitter-stream.yaml", "./setup.sh", "./depl.sh"]

def changeText(revert=False):
    reqList = ["kvPairs.txt"]
    missing_file_list = list(f for f in reqList if f not in os.listdir(os.curdir))
    if len(missing_file_list):
        print("Missing file(s) ", missing_file_list)
        return None
    fd_list = list(open(fn,"r") for fn in filenames)
    txt_list = list(fd.read() for fd in fd_list)

    fd3 = open("./kvPairs.txt")
    kv_pairs = []
    for line in fd3.readlines():
        line = line.strip()
        line=line.split("=")
        kv_pairs.append(line)
        # for i in range(len(txt_list)):
        #     if(revert):
        #         txt_list[i] = txt_list[i].replace(line[1], line[0])
        #     else:
        #         txt_list[i] = txt_list[i].replace(line[0],line[1])
        # print(line)
    #replace longer sequences first to avoid conflicts
    kv_pairs=sorted(kv_pairs, key=lambda x:len(x[1]),reverse=True)
    for line in kv_pairs:
        for i in range(len(txt_list)):
            if(revert):
                txt_list[i] = txt_list[i].replace(line[1], line[0])
            else:
                txt_list[i] = txt_list[i].replace(line[0],line[1])
        print(line)
    for fd in fd_list:
        fd.close()
    fd3.close()
    fd_list = list(open(fn,"w") for fn in filenames)
    for i in range(len(fd_list)):
        fd = fd_list[i]
        fd.write(txt_list[i])
        fd.flush()
        fd.close()


if __name__ == "__main__":
    changeText()