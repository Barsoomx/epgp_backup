#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import re
from datetime import datetime
import logging
import pandas as pd
import os
import io
import numpy as np
import sys
import lupa
from lupa import LuaRuntime
import dateparser
from pathlib import Path

COMMASPACE =','

def is_main(member):
    if COMMASPACE in member:
        return True

def newest(path):
    path = str(path)
    files = os.listdir(path)
    paths = [os.path.join(path, basename) for basename in files]
    try:
        m = max(paths, key=os.path.getctime)
    except ValueError:
        m = None
    return m


def main():
    try:
        gname=sys.argv[1]
    except IndexError:
        gname = 'Traitors of humanity'
    p = Path(f"./epgp_backups/{gname}/")
    p.mkdir(parents=True, exist_ok=True)
    with open('./SavedVariables/epgp.lua', 'r', encoding='utf-8') as fh:
        txt = fh.read()
    lua = LuaRuntime(unpack_returned_tuples=True)
    table = lua.eval(txt.replace("EPGP_DB = ",""))
    roster_info=table.namespaces.log.profiles[gname].snapshot.roster_info
    roster_time=table.namespaces.log.profiles[gname].snapshot.time
    guild_info = table.namespaces.log.profiles[gname].snapshot.guild_info
    BASE_GP = int(re.search(r'@BASE_GP:(\d+)', guild_info).group(1))
    ts = datetime.utcfromtimestamp(int(roster_time)).strftime('%Y-%m-%dT%H-%M-%S')
    latest = newest(p.absolute())
    csv = f'{p.absolute()}/{gname}-{ts}.csv'
    if csv == latest:
        print(f'Already latest backup, {csv}')
        exit(0)

    backup=[]
    for subtable in roster_info.values():
        name, klass, note = list(subtable.values())
        if is_main(note):
            try:
                EP , GP, freeze = re.match(r'(\d+)\.?,(\d+)\.?\s?(.*)', note).groups()
                EP, GP = int(EP), int(GP)
                PR = float(EP)/(float(GP)+BASE_GP)

                try:
                    freezedate_str = re.search(r'(\d+[ .\/,]\d+[ .\/,]?\d*)', freeze).group(1)
                    freezedate = dateparser.parse(freezedate_str, date_formats=['%d/%m','%d/%m/%y','%d.%m','%d.%m.%y','%d %m', '%d %m %y', '%d,%m','%d,%m,%y','%d,%m'])
                except:
                    freezedate = None
                    if freeze != "":
                        print(f"freeze date not found for note {freeze}: {name}")
                backup.append([name,EP,GP,PR, freezedate, freeze])
            except Exception as e:
                print("error parsing",e)

    df=pd.DataFrame(data=backup)

    df.astype({1: np.float64, 0: str, 2: np.float64, 3: np.float64, 4: "datetime64", 5: str}, errors='raise')
    df_sorted=df.sort_values([4,3],ascending=[True,False])

    outputStream = io.StringIO()
    df_sorted.to_csv(outputStream,index=False, encoding='utf-8-sig',header=False)

    if latest is not None:
        with open(latest,'r',encoding='utf-8-sig') as last:
            if last.read() == outputStream.getvalue():
                outputStream.close()
                print('Backup aborted, nothing changed')

            else:
                df_sorted.to_csv(csv,index=False, encoding='utf-8-sig',header=False)

    else:
        df_sorted.to_csv(csv,index=False, encoding='utf-8-sig',header=False)

if __name__ == '__main__':
    main()
