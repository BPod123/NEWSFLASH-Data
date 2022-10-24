"""
Parameter 1: data_directory - Absolute path of directory with files subdirectories to merge.
Parameter 2: sources_path - Absolute path of Sources.csv
Parameter 3: storage_path - Storage File Name (File name to store database in) 
 
About the directory organization:
Top
    \
    subDir1,  subDir2, ...
    In each Sub directory: Folders with names of years that the data was collected.
    
    In each folder: csv files that match the names of sources in Sources.csv
    
Database Schema:

Tables: headline, feed

feed: url (PK), name

headline:  url (PK, FK), publish date (PK, Index), title, description
    
"""
import argparse
import pandas as pd
import os
import sqlite3
from time import time
from tqdm import tqdm
import numpy as np
def getDf(fileName, url):
    df = pd.read_csv(fileName)
    df.rename(columns={"DATE": "date", "TITLE": "title", "SUMMARY": "description"}, inplace=True)
    df["url"] = url
    if df['title'].isnull().values.any():
        nullTitles = df.loc[df['title'].isnull()]
        nullTitles['title'] = nullTitles['description']
        nullTitles['description'] = str(np.NAN)
        df[df['title'].isnull()] = nullTitles
        df = df[df['title'].notna()]
    return df
def appendHeadlines(filesAndUrls: list, con):
    cutoff = 16384
    dfs = []
    size = 0
    filesInQueue = 0
    filesProcessed = 0
    headLinesStored = 0
    start = time()
    iterator = tqdm(range(len(filesAndUrls)))
    iterator.set_description(f"Files Processed: {0}/{len(filesAndUrls)}\tHeadlines Stored: {0} ({0} headlines per second)")
    for i in iterator:
        file, url = filesAndUrls[i]
        dfs.append(getDf(file, url))
        filesInQueue += 1
        size += len(dfs[-1])
        if size > cutoff or i == len(filesAndUrls) - 1:
            combined = pd.concat(dfs)
            dfs.clear()
            combined.drop_duplicates(inplace=True)
            if len(combined) < cutoff and i != len(filesAndUrls) - 1:
                dfs.append(combined)
                size = len(combined)
            else:
                combined.reset_index(inplace=True)
                combined.drop(columns=['index'], inplace=True)

                appendHeadlinesHelper(combined, con)
                elapsed = time() - start
                filesProcessed += filesInQueue
                filesInQueue = 0
                headLinesStored += len(combined) # Note len(combined) != size
                size = 0
                headLinesPersecond = int(headLinesStored // elapsed)
                iterator.set_description(f"Files Processed: {filesProcessed}/{len(filesAndUrls)}\tHeadlines Stored: {headLinesStored} ({headLinesPersecond} headlines per second)")
def appendHeadlinesHelper(df, con):
    try:
        df.to_sql("headline", con, if_exists="append", index=False)
    except sqlite3.IntegrityError as error:
        if "UNIQUE constraint failed" in error.args[0]:
            if len(df) <= 1:
                return
            # At least one of the entries is already in the database.
            # Must try to add each entry in df individually so that new data will
            # not be skipped. Can use divide and conquer to save time
            firstHalf = df.iloc[:len(df) // 2]
            secondHalf = df.iloc[len(df) - len(df) // 2:]
            appendHeadlinesHelper(firstHalf, con)
            appendHeadlinesHelper(secondHalf, con)
        else:
            raise error



if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("data_directory", help="Absolute path of directory with files subdirectories to merge")
    parser.add_argument("sources_path", help="Absolute path of Sources.csv")
    parser.add_argument("storage_path", help="Storage file name (file name to store database under)")
    args = parser.parse_args()
    dataPath, sourcesPath, storagePath = args.data_directory, args.sources_path, args.storage_path
    sources = pd.read_csv(sourcesPath)
    sources.drop(columns=["PUBLISH_FREQUENCY"], inplace=True)
    sources.rename(columns={"NAME": "name", "RSS_URL": "url"}, inplace=True)
    con = sqlite3.connect(storagePath)
    cursor = con.cursor()
    # Create feeds table and store sources
    tables = [x[0] for x in cursor.execute("SELECT name FROM sqlite_master")]
    if "feed" not in tables:
        cursor.execute("""CREATE TABLE feed 
        (
        name TEXT,
        url TEXT NOT NULL,
        PRIMARY KEY(url)
        )
        """)
        sources.to_sql("feed", con, index=False, if_exists="append")
    if "headline" not in tables:
        cursor.execute("""CREATE TABLE headline
        (
        url TEXT NOT NULL,
        date TIMESTAMP NOT NULL,
        title TEXT NOT NULL,
        description TEXT,
        PRIMARY KEY (url, date, title),
        FOREIGN KEY (url) REFERENCES feed(url)
        )
        """)
        cursor.execute("CREATE INDEX headline_date_index ON headline (date)")
    cursor.close()
    # Walk through data directory and collect csv file names and pair with source names
    filesAndUrls = []
    for tup in os.walk(dataPath):
        for fName in tup[2]:
            if fName.endswith('.csv'):
                matchingSource = sources.loc[sources["name"] == fName[:-4]]
                if len(matchingSource) == 0:
                    continue
                elif len(matchingSource) > 1:
                    raise Exception("Multiple sources found with same name: ", matchingSource)
                url = matchingSource.reset_index()["url"][0]
                fileName = os.path.join(tup[0], fName)
                filesAndUrls.append((fileName, url))
    appendHeadlines(filesAndUrls, con)
    con.commit()
    con.close()
