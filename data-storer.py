from imports import storage
from imports import inputRetriever

NUM_EVENTS = 8


def main():
    print("hello world")
    ir = inputRetriever.InputRetriever()
    stor = storage.Storage()

    input_files = ir.getInputFilesList()

    for file in input_files:
        fd = open(file, 'r')
        for index,line in enumerate(fd):
            res = processLine(line)
            if(res[0] > 0):
                print('Error occurred while parsing line %d in file %s : %s' % (index,file.split('/')[-1],res[1]))
        fd.close()

def processLine(line):

    line_tok = line.split(',')

    if len(line_tok) <3:
        return 1, 'not enough fields'

    #todo: check if 1st field (TS) is numeric

    event_type = 0
    try:
        event_type = int(line_tok[2])
    except ValueError as e:
        print('E: ',e.args )
        return 2, 'event type field is not a number'


    if(event_type >= 0 and event_type < NUM_EVENTS):
        res = __manageCase(line_tok,event_type)
    else:
        return 3, 'no meaningful event type'

    if res[0] > 0:
        return res[1],res[2]



    return 0, None



#todo
def __manageCase(tokenized_line,event_type):

    if(event_type == 0):
        pass
    if (event_type == 1):
        pass
    if (event_type == 2):
        pass
    if (event_type == 3):
        pass
    if (event_type == 4):
        pass
    if (event_type == 5):
        pass
    if (event_type == 6):
        pass
    if (event_type == 7):
        pass

    return 0,None



if __name__ == "__main__":
    main()
