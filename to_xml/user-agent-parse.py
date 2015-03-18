#!/usr/bin/env python
from itertools import *
mobileAgent = ["blackberry", "cldc", "hp", "htc", "iemobile", "kindle", "midp", "mmp", "motorola", "mobile", "nokia", "opera mini", "opera", "Googlebot-Mobile", "YahooSeeker", "android", "iphone", "ipod", "mobi", "palm", "pocket", "portalmmm", "ppc", "smartphone", "sonyericsson", "sqh", "spv", "symbian", "treo", "up.browser", "up.link", "vodafone", "windows ce","xda"]

def readRec(file):
    for line in file:
        yield line.strip().split('\t')

def isMobile(x):
    for agent in mobileAgent:
        if(agent.upper() in x.upper()):
            return True
    return False


def main():
    total = 0;
    totalMobile = 0;
    recFile = open("browser.txt", 'r')
    data = readRec(recFile)
    for i in data:
        total += int(i[1])
        if(isMobile(i[0])):
            totalMobile += int(i[1])
    print "total: %d" % total
    print "from Mobile: %d" % totalMobile

if __name__ == "__main__":
    main()
