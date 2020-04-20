import pandas as pd
import os
from domain import Tap


def readByActivityID(df, activity_id):
    return df[df["ActivityID"].isin([activity_id])]


def segmentForTouchEvent(touchEventData, group=[[0, 5], [1, 6], [2]]):
    result = pd.DataFrame(columns=('startTime', 'endTime'))
    startTime = ""
    endTime = ""
    for tup in zip(touchEventData['Systime'], touchEventData['ActionID']):
        if int(tup[1]) in group[0]:
            startTime = tup[0]
        if int(tup[1]) in group[2]:
            endTime = tup[0]
        if startTime != "" and endTime != "":
            result = result.append(pd.DataFrame({"startTime": startTime, "endTime": endTime}, index=["0"]),
                                   ignore_index=True)
            startTime = ""
            endTime = ""
    return result


def generateTapEntityList(accData, segData):
    results = []
    for tup in zip(segData['startTime'], segData['endTime']):
        needCols = accData.loc[:, ['Systime', 'X', 'Y', 'Z']]
        before100 = needCols[(needCols.Systime >= tup[0] - 100) & (needCols.Systime < tup[0])]
        after200 = needCols[(needCols.Systime > tup[1]) & (needCols.Systime <= tup[1] + 200)]
        during_tap = needCols[(needCols.Systime >= tup[0]) & (needCols.Systime <= tup[1] + 200)]
        results.append(Tap(before100[['X', 'Y', 'Z']].values,
                           during_tap[['X', 'Y', 'Z']].values,
                           after200[['X', 'Y', 'Z']].values))
    return results


def getTapListByPathAndActivityID(path='/Users/yumei.hou/Documents/data-storage/public_dataset/100669/100669_session_1',
                                  activity_id='100669011000001'):
    touchEventData = readTouchEventData(path)
    accData = readAccData(path)

    touchEventData = readByActivityID(touchEventData, activity_id)
    accData = readByActivityID(accData, activity_id)

    segData = segmentForTouchEvent(touchEventData)

    TapList = generateTapEntityList(accData, segData)

    return TapList


def readAccData(path):
    accDataType = {"Systime": "int64", "ActivityID": "object", "X": "float64", "Y": "float64", "Z": "float64"}
    accData = pd.read_csv(os.path.join(path, "Accelerometer.csv"), usecols=[0, 2, 3, 4, 5], header=-1,
                          names=["Systime", "ActivityID", "X", "Y", "Z"], dtype=accDataType)
    return accData


def readTouchEventData(path):
    touchEventDataType = {"Systime": "int64", "ActivityID": "object", "ActionID": "object"}
    touchEventData = pd.read_csv(os.path.join(path, "TouchEvent.csv"), usecols=[0, 2, 5], header=-1,
                                 names=["Systime", "ActivityID", "ActionID"], dtype=touchEventDataType)
    return touchEventData
