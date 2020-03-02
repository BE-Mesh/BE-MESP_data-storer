from imports import storage
from imports import inputRetriever
import re

NUM_EVENTS = 8


def main():
    print("hello world")
    ir = inputRetriever.InputRetriever()
    stor = storage.Storage()

    input_files = ir.getInputFilesList()
    print('**********************')
    for file in input_files:
        fd = open(file, 'r')
        for index,line in enumerate(fd):
            res = processLine(line.rstrip())
            if(res[0] > 0):
                print('Error occurred while parsing line %d in file %s : %s' % (index,file.split('/')[-1],res[1]))
        fd.close()

    stor.close()

def processLine(line):

    line_tok = line.split(',')

    if len(line_tok) <3:
        return 2, 'not enough fields'

    #todo: check if 1st field (TS) is numeric

    event_type = 0
    try:
        event_type = int(line_tok[2])
    except ValueError as e:
        print('E: ',e.args )
        return 3, 'event type field is not a number'


    if(event_type >= 0 and event_type < NUM_EVENTS):
        res = __manageCase(line_tok,event_type)
    else:
        return 4, 'no meaningful event type'

    if res[0] > 0:
        return res[0],res[1]



    return 0, None



def __manageCase(tokenized_line,event_type):

    timestamp = tokenized_line[0]
    ev_submitter_id = tokenized_line[1]
    stor = storage.Storage()
    res = 1,'default'

    if(event_type == 0):
        # Message Sent
        if len(tokenized_line) < 9:
            return 5, 'event-type \'Message Sent\' requires more arguments'

        try:
            int(tokenized_line[6])
        except ValueError:
            return 6, 'event-type \'Message Sent\' needs an integer value for message_type field'

        if tokenized_line[7].upper() != 'NULL':
            try:
                int(tokenized_line[7])
            except ValueError:
                return 6, 'event-type \'Message Sent\' needs an integer value, or \'NULL\', for sequence_number field'

        else:
            tokenized_line[7] = 'NULL'

        res = stor.storeMessageSentEvent(ts=tokenized_line[0],submitter_id=tokenized_line[1],sender=tokenized_line[3],
                                         receiver=tokenized_line[4],next_hop=tokenized_line[5],
                                         message_type=tokenized_line[6],sequence_number=tokenized_line[7],
                                         payload=tokenized_line[8])

    elif (event_type == 1):
        # Message Received
        if len(tokenized_line) < 9:
            return 5, 'event-type \'Message Received\' requires more arguments'

        try:
            int(tokenized_line[6])
        except ValueError:
            return 6, 'event-type \'Message Received\' needs an integer value for message_type field'

        if tokenized_line[7].upper() != 'NULL':
            try:
                int(tokenized_line[7])
            except ValueError:
                return 6, 'event-type \'Message Received\' needs an integer value, or \'NULL\', for sequence_number field'
        else:
            tokenized_line[7] = 'NULL'


        res = stor.storeMessageRcvEvent(ts=tokenized_line[0], submitter_id=tokenized_line[1], sender=tokenized_line[3],
                                         receiver=tokenized_line[4], prev_hop=tokenized_line[5],
                                         message_type=tokenized_line[6], sequence_number=tokenized_line[7],
                                         payload=tokenized_line[8])

    elif (event_type == 2):
        # Outgoing Connection Attempt
        if len(tokenized_line) < 4:
            return 5, 'event-type \'Outgoing Connection Attempt\' requires more arguments'
        # todo: add eventual checks to each field

        res = stor.storeOutgoingConnectionAttemptEvent(ts=tokenized_line[0], submitter_id=tokenized_line[1],
                                                       connect_to=tokenized_line[3])

    elif (event_type == 3):
        # Incoming Connection Attempt
        if len(tokenized_line) < 4:
            return 5, 'event-type \'Incoming Connection Attempt\' requires more arguments'
        # todo: add eventual checks to each field

        res = stor.storeIncomingConnectionAttemptEvent(ts=tokenized_line[0], submitter_id=tokenized_line[1],
                                                       connect_from=tokenized_line[3])

    elif (event_type == 4):
        # Connection Attempt Result
        if len(tokenized_line) < 6:
            return 5, 'event-type \'Connection Attempt Result\' requires more arguments'
        if not(tokenized_line[5] in ['A','a','R','r']):
            return 6, 'event-type \'Connection Attempt Result\' needs a value A or R for outcome field '

        res = stor.storeConnectionAttemptResultEvent(ts=tokenized_line[0], submitter_id=tokenized_line[1],
                                                     connect_from=tokenized_line[3],connect_to=tokenized_line[4],
                                                     outcome=tokenized_line[5].upper())

    elif (event_type == 5):
        # Device Up
        if len(tokenized_line) < 3: #dummy
            return 5, 'event-type \'Device Up\' requires more arguments'

        res = stor.storeDeviceUpEvent(ts=tokenized_line[0], submitter_id=tokenized_line[1])

    elif (event_type == 6):
        # Assume Role
        if len(tokenized_line) < 4:
            return 5, 'event-type \'Assume Role\' requires more arguments'

        if not(tokenized_line[3] in ['C','c','S','s']):
            return 6, 'event-type \'Assume Role\' needs a value C or S for role field '

        res = stor.storeAssumeRoleEvent(ts=tokenized_line[0], submitter_id=tokenized_line[1], role=tokenized_line[3].upper())

    elif (event_type == 7):
        # Scan
        if len(tokenized_line) < 4:
            return 5, 'event-type \'Scan\' requires more arguments'

        if not(tokenized_line[3] in ['E','e','S','s']):
            return 6, 'event-type \'Scan\' needs a value S or E for status field '

        res = stor.storeScanEvent(ts=tokenized_line[0], submitter_id=tokenized_line[1], status=tokenized_line[3].upper())




    else:
        res = 9,'error in __manageCase' #this should be never happen (event_type check at upper level

    return res



if __name__ == "__main__":
    main()
