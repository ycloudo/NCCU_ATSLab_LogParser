import pandas as pd
import json
import os

class LogParser:
    '''
    @param fpath The path of cowrie.json
    '''
    def __init__(self,outdir):
        self.output_dir =outdir
        
    def Json2Log(self,fpath):
        # parse every log by session id
        logs = []
        self.fname = fpath.split('/')[-2]
        with open(fpath, 'r') as file:
            for line in file:
                if line[0:3] == "app":
                    line = line[3:]
                logs.append(json.loads(line))

        # Convert the list of JSON objects into a DataFrame
        self.main_data = pd.DataFrame(logs)

    def parsing(self):
        grouped = self.main_data.groupby("session").agg(list).reset_index()
        self.result = []
        for i in range(len(grouped)):
            tmp_obj = {
                "session_id": grouped.loc[i,"session"],
                "duration": grouped.loc[i,"duration"][-1],
                "events":[]
            }
            for j in range(len(grouped.loc[i,"eventid"])):
                tmp_event = {
                    "timestamp": grouped.loc[i,"timestamp"][j],
                    "event_id": grouped.loc[i,"eventid"][j],
                    "src_ip": grouped.loc[i,"src_ip"][j],
                    "message": grouped.loc[i,"message"][j],
                }
                tmp_obj["events"].append(tmp_event)
            self.result.append(tmp_obj)

    def output(self):
        os.makedirs(self.output_dir, exist_ok=True)
        with open(f'{self.output_dir}/{self.fname}.json', 'w') as json_file:
            json.dump(self.result, json_file, indent=4)