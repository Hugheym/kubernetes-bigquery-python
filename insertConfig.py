filenames = ["./pubsub/bigquery-controller.yaml","./pubsub/twitter-stream.yaml", "./setup.sh", "./depl.sh"]

def changeText(revert=False):
    fd_list = list(open(fn,"r") for fn in filenames)
    txt_list = list(fd.read() for fd in fd_list)

    fd3 = open("./kvPairs.txt")
    for line in fd3.readlines():
        line = line.strip()
        line=line.split("=")
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