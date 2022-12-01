import pymqi

queue_manager = pymqi.connect('PRACTICUM', 'PRACTICUM.SVRCONN', '10.100.1.20(31414)')

q = pymqi.Queue(queue_manager, 'PRACTICUM.LQ')
q.put('Hello from Python!')
