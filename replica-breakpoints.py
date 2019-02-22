import sys
import socket
import mysql.connector
import re

def phase_one():
    got_statement = False
    while not got_statement:
        (data, address)  = sock.recvfrom(1024)
        statements = data.decode()
        if len(statements) > 3:
            got_statement = True
    statements_split = statements.split(';')
    try:
        for line in statements_split:
            print(line)
            cursor.execute(line)
        print('prepared, voting aye')
        sys.stdin.readline()
        # fail point 1: after preparation, before voting...
        # this is addressed by coordinator re-sending statement
        sock.sendto('aye'.encode('ASCII'), address)
        print('sent vote')
        log = open('log', 'w')
        log.write(statements)
        log.close()
    except KeyboardInterrupt:
        print('ctrl-c KeyboardInterrupt...')
        sys.exit()
    except:
        print('caught exception, voting nay')
        sys.stdin.readline()
        # fail point 1: after preparation, before voting...
        # this is addressed by coordinator re-sending statement
        sock.sendto('nay'.encode('ASCII'), address)
        print('sent vote')
        log = open('log', 'w')
        log.write('nay')
        log.close()
    phase_two()

def phase_two():
    print('waiting for coordinator\'s command')
    sys.stdin.readline()
    # fail point 2: after voting, before getting/executing command...
    # this is addressed by reading statement or 'nay' in log upon recovery,
    # preparing statement again and jumping to phase_two() to wait for
    # command being re-sent by coordinator, or not preparing anything and
    # just jumping to phase_two()
    got_command = False
    while not got_command:
        (data, address)  = sock.recvfrom(1024)
        command = data.decode()
        if len(command) == 3:
            got_command = True
    print('received command: {}'.format(command))
    if command == 'com':
        cnx.commit()
        print('committed')
        log = open('log', 'a')
        log.write('\n' + command)
        log.close()
        print('sending acknowledgement')
        sys.stdin.readline()
        # fail point 3: after getting/executing command, before sending ack...
        # this is addressed by reading statement and command in the log, or
        # 'nay' and command, then sending ack, clearing log, and jumping
        # to phase_one()
        sock.sendto('ack'.encode('ASCII'), address)
    elif command == 'abo':
        cnx.rollback()
        print('aborted')
        log = open('log', 'a')
        log.write('\n' + command)
        log.close()
        print('sending acknowledgement')
        sys.stdin.readline()
        # fail point 3: after getting/executing command, before sending ack...
        # this is addressed by reading statement and command in the log, or
        # 'nay' and command, then sending ack, clearing log, and jumping
        # to phase_one()
        sock.sendto('ack'.encode('ASCII'), address)
    log = open('log', 'w')
    log.write('')
    log.close()
    print('ack send, log cleared')
    phase_one()

# main
port = 9999
sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock.bind(('', port))
print('listening on port {}'.format(str(port)))

cnx = mysql.connector.connect(user='root',
                              password=sys.argv[1],
                              database='yelp_db')
cursor = cnx.cursor()
log = open('log', 'r')
leftovers = log.readlines()
log.close()
if len(leftovers) == 0:
    print('nothing in log, starting at phase_one()')
    phase_one()
elif len(leftovers) == 1:
    print('found transaction voted on, redoing and joining phase_two()')
    if leftovers[0] == 'nay':
        phase_two()
    else:
        statements_split = leftovers[0].split(';')
        for line in statements_split:
            print(line)
            cursor.execute(line)
        phase_two()
elif len(leftovers) == 2:
    print('found completed transaction but ack wasn\'t sent')
    (data, address)  = sock.recvfrom(1024)
    print('sending acknowledgement')
    sock.sendto('ack'.encode('ASCII'), address)
    log = open('log', 'w')
    log.write('')
    log.close()
    print('ack sent, log cleared')
    phase_one()
