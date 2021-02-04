from datetime import datetime
from dateutil import parser
import statistics

class ThreadProcess():
    def __init__(self):
        self.file_name = ''
        # ChildDic: {ProcessID: ThreadID}
        self.ChildDic = {}
        # ParentDic: {ThreadID: ProcessID}
        self.ParentDic = {}
        # Thread2Time: {ThreadID: [Start, End]}
        self.Thread2Time = {}
        # Time2Thread: {[Start, End]: ThreadID}
        self.Time2Thread = {}

    # If required, read log file and parse the text format into standard format, 
    # else we could embed this step into differnt operations 
    # we can apply hadooop mapreduce framework to precoss log files at the same time
    def preprocess(self):
        log = open(self.file_name, 'r')
        for line in log:
            line1 = line.split('::') 

            # P_T: Procoss_ID, Thread_ID 
            P_T = line1[0]
            cur_PID, cur_TID = P_T.split(':')[0],P_T.split(':')[1] 
            self.ChildDic[cur_PID].append(cur_TID)
            self.ParentDic[cur_TID].append(cur_PID)

            # name_time_state: Thread_Name, Logged_Time, Logged_Message
            name_time_state = line1[1]
            date, time, state = name_time_state.split(' ')[1], name_time_state.split(' ')[2],name_time_state.split(' ')[4]
            
            # convert time to stabdard datetime format 
            year, month, day = date.split('-')[0], date.split('-')[1], date.split('-')[2]
            hour, minute, second = time.split(':')[0], time.split(':')[1], time.split(':')[2]
            cur_time = datetime(year, month, day, hour, minute, second)

            if state == '**START**' or state == '**END**':
                self.Thread2Time[cur_TID].append(cur_time)

    # Use inverted index idea in hadoop word count algorithm
    def invertIndex(self):
        self.Time2Thread = dict(zip(self.Thread2Time.values(), self.Thread2Time.keys()))

    # Basic API: return numbers of active threads, ThreadID and ProcessID
    def activeThread(self, t1, t2):
        # convert input to standard format
        pattern = '{pId}:::{threadId}:::{startTime}:::{endTime}'
        begin = datetime.strptime(t1, '%Y-%m-%d %H:%M:%S')
        end = datetime.strptime(t2, '%Y-%m-%d %H:%M:%S')

        # res_TID: save ID of all active threads
        res_TID = []
        # res PID: PID of active threads
        res_PID = []
        # relevant: references to the files which stores the logs of these threads.
        relevant = []

        log = open(self.file_name, 'r')
        while True:
            # we only focus on start and end time of every thread here
            line = log.readline()
            parse_line = parser.parse(pattern, line)
            cur_start = datetime.strptime(parse_line['startTime'].split(',')[0], '%Y-%m-%d %H:%M:%S')
            cur_end = datetime.strptime(parse_line['endTime'].split(',')[0], '%Y-%m-%d %H:%M:%S')

            # We assume that the returned thread must be alsways active from given start to given end time
            if cur_start <= begin and cur_end >= end:
                res_TID.append(parse_line['threadId'])
                res_PID.append(parse_line['pId'])
                relevant.append(line[:-1])

        return len(set(res_TID)), res_TID, res_PID, relevant

    # Bunus API1: return highest count of concurrent threads running in any second, and that second.
    def highestConThread(self):
        pattern = '{pId}:::{threadId}:::{startTime}:::{endTime}'
        # countThread: {second: numbers of threads at this second}
        countThread = {}
        TimeMaxThreads = str()

        log = open(self.file_name, 'r')
        while True:
            line = log.readline()
            parse_line = parser.parse(pattern, line)
            cur_start = datetime.strptime(parse_line['startTime'].split(',')[0], '%Y-%m-%d %H:%M:%S')
            cur_end = datetime.strptime(parse_line['endTime'].split(',')[0], '%Y-%m-%d %H:%M:%S')

            # cumulative add current thread into countThread dict in the range of current start time to current end time
            while cur_start < cur_end:
                if str(cur_start) in countThread.keys():
                    countThread[str(cur_start)] += 1
                else:
                    countThread[str(cur_start)] = 1
                cur_start += 1

        # obtain second time(key) according to thread numbers(value)
        TimeMaxThreads = max(countThread, key = countThread.get)
        # corresponding max count of thread
        highestConThreadNum = countThread[TimeMaxThreads]
        return TimeMaxThreads, highestConThreadNum
    
    # Bunus API2: average and stdev of the all threads lifetime
    def avg_and_stdev(self):
        pattern = '{pId}:::{threadId}:::{startTime}:::{endTime}'
        # lifetime: total running time of every thread
        lifetime = []
        avg, stdev = 0, 1

        log = open(self.file_name, 'r')
        while True:
            line = log.readline()
            parse_line = parser.parse(pattern, line)
            cur_start = datetime.strptime(parse_line['startTime'].split(',')[0], '%Y-%m-%d %H:%M:%S')
            cur_end = datetime.strptime(parse_line['endTime'].split(',')[0], '%Y-%m-%d %H:%M:%S')

            # calculate the running time: end - start
            totalTime = (cur_end - cur_start).total_seconds()
            lifetime.append(totalTime)

        avg, stdev = statistics.mean(lifetime), statistics.stdev(lifetime)
        return avg, stdev

# Suggestions of improving system: 
# Automatic anomaly detection and alerts by unsupervised learning methods
# Real-time flushing and standard stream processing when the thread ends
# According to requirements, we could decide whether we need to do extra data processing

