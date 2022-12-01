import pymqi

queue_manager = pymqi.connect('PRACTICUM', 'PRACTICUM.SVRCONN', '10.100.1.20(31414)')

q = pymqi.Queue(queue_manager, 'PRACTICUM.LQ')
msg = q.get()
print('Here is the message:', msg)
