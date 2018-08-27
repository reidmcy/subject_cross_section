print("importing")
import nltk
import parsl

import os
import csv
import json
import logging

import parsl_configs

subjectsDir = 'data'
cleanedFile = 'processed.tsv'

maxRunning = 256
perBatch = 50

def subjectIter():
    for e in os.scandir(subjectsDir):
        if e.name.endswith('.tsv'):
            with open(e.path) as f:
                reader = csv.DictReader(f, delimiter = '\t')
                for row in reader:
                    yield {
                        'wos_id' : row['wos_id'],
                        'title' : row['title'],
                        'abstract' : row['abstract'],
                        }
    return

def gen_full_tokenizer(dfk):
    #Start the parsl generator instead of using the wrapper syntax
    return parsl.App('python', dfk)(tokenizeEntry)


def tokenizeEntry(entries):
    import nltk

    def tokenizer(target):
        try:
            return nltk.word_tokenize(str(target).lower())
        except:
            return []
            #import pdb; pdb.set_trace()

    def sentinizer(sent):
        try:
            return [tokenizer(s) for s in nltk.sent_tokenize(sent)]
        except TypeError:
            #Missing abstract
            return []

    ret = []
    for e in entries:
        ret.append({
                'wos_id' : e['wos_id'],
                'title' : tokenizer(e['title']),
                'abstract' : sentinizer(e['abstract']),
                })
    return ret

def checkRunning(running):
    completesNameLists = [k for k, v in running.items() if v.done()]
    succCount = 0
    if len(completesNameLists) > 0:
        with open(cleanedFile, 'a') as f:
            for name in completesNameLists:
                try:
                    results = running.pop(name).result()
                except Exception as e:
                    print(str(e)) + '\n' + str(traceback.format_exc())
                else:
                    for e in results:
                        f.write(json.dumps(e))
                        f.write('\n')
                    succCount += len(results)
    return succCount

def main():
    print("Loading DFK")
    parsl.set_file_logger("parsl.log", level=logging.DEBUG)

    dfk = parsl.DataFlowKernel(config=parsl_configs.localNode)

    print("Loading App")
    full_app = gen_full_tokenizer(dfk)

    print("Loading data iter")
    datIter = subjectIter()

    print("Starting run")

    running = {}
    done = False
    succCount = 0
    doneIter = False
    try:
        while not done:
            #Only add maxRunning jobs to the queue at once
            while len(running) < maxRunning:
                batch = []
                for i in range(perBatch):
                    try:
                        batch.append(next(datIter))
                    except StopIteration:
                        #If none left just skip adding
                        doneIter = True
                        break
                batchName = batch[0]['wos_id']
                running[batchName] = full_app(batch)
            succCount += checkRunning(running)
            print("Completed {}".format(succCount))
            #End the loop if all jobs are done and no more can be added
            if doneIter and len(running) < 1:
                done = True

    except KeyboardInterrupt:
        print("Closing down")
        dfk.cleanup()
        raise

    dfk.cleanup()
    print("Done")

if __name__ == '__main__':
    main()
