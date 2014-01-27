import os
import random
import string

from multiprocessing import Process

import constants


class FileWriter:

    def __init__(self, tableDirectory, fname, separator='|'):
        self.fname = os.path.join(tableDirectory, fname + '.tbl')
        self.fh = None
        self.separator = separator

    def save(self, vlist):
        if not self.fh:
            self.fh = open(self.fname, 'w')
        if isinstance(vlist, basestring):
            self.fh.write(vlist+'\n')
        else:
            self.fh.write(self.separator.join([str(v) for v in vlist])+'\n')

    def close(self):
        self.fh.close()


class Generator:

    def __init__(self, tbldir, scalefactor):
        self.tbldir = tbldir
        self.subscribers = constants.BASE_POPULATION * scalefactor
        self.subscriberDataWriter = FileWriter(tbldir, constants.TABLENAME_SUBSCRIBER)
        self.accessDataWriter = FileWriter(tbldir, constants.TABLENAME_ACCESS_INFO)
        self.facilityDataWriter = FileWriter(tbldir, constants.TABLENAME_SPECIAL_FACILITY)
        self.forwardingDataWriter = FileWriter(tbldir, constants.TABLENAME_CALL_FORWARDING)

    def deleteExistingTablefiles(self):
        for tblname in ['%s.tbl' % tbl for tbl in constants.ALL_TABLES]:
            try:
                os.unlink(os.path.join(self.tbldir, tblname))
            except OSError as e:
                if e.errno == 2: #FileNotFound
                    print 'Trying to delete {}. File not found. Skipping.'.format(tblname)

    def execute(self):
        print 'Generating data for a base population of %d subscribers' % self.subscribers
        p1 = Process(target=generate_subscriber_data, kwargs={'tbldir':self.tbldir, 'subscribers':self.subscribers})
        p2 = Process(target=generate_access_info_data, kwargs={'tbldir':self.tbldir, 'subscribers':self.subscribers})
        p3 = Process(target=generate_facility_and_forwarding_data, kwargs={'tbldir':self.tbldir, 'subscribers':self.subscribers})
        for p in [p1,p2,p3]:
            p.start()
        for p in [p1,p2,p3]:
            p.join()

def generate_subscriber_data(subscribers, tbldir):
    writer = FileWriter(tbldir, constants.TABLENAME_SUBSCRIBER)
    writer.save(constants.HEADERS[constants.TABLENAME_SUBSCRIBER])
    assert len(str(subscribers)) <= 15, "the field sub_nbr is a 15 digit string but s_id has more than 15 digits"
    for s_id in xrange(subscribers):
        sub_nbr = "{0:015d}".format(s_id+1)
        bit_X = random.choice([0,1])
        hex_X = random.randrange(16)
        byte2_X = random.randrange(256)
        msc_location = random.randrange(4294967295)+1 #(between 1 and 2^32-1)
        vlr_location = random.randrange(4294967295)+1
        writer.save([s_id+1, sub_nbr] + [random.choice([0,1]) for i in range(10)] + [random.randrange(16) for i in range(10)] + [random.randrange(256) for i in range(10)] + [msc_location, vlr_location])
    writer.close()

def generate_access_info_data(subscribers, tbldir):
    writer = FileWriter(tbldir, constants.TABLENAME_ACCESS_INFO)
    writer.save(constants.HEADERS[constants.TABLENAME_ACCESS_INFO])
    for s_id in xrange(subscribers):
        for i in random.sample([1,2,3,4],random.randint(1,4)):
            ai_type = i
            data1 = random.randrange(256)
            data2 = random.randrange(256)
            data3 = ''.join(random.choice(string.ascii_uppercase) for i in range(3))
            data4 = ''.join(random.choice(string.ascii_uppercase) for i in range(5))
            writer.save([s_id+1, ai_type, data1, data2, data3, data4])
    writer.close()

def generate_facility_and_forwarding_data(subscribers, tbldir):
    facility_writer = FileWriter(tbldir, constants.TABLENAME_SPECIAL_FACILITY)
    facility_writer.save(constants.HEADERS[constants.TABLENAME_SPECIAL_FACILITY])
    forwarding_writer = FileWriter(tbldir, constants.TABLENAME_CALL_FORWARDING)
    forwarding_writer.save(constants.HEADERS[constants.TABLENAME_CALL_FORWARDING])
    for s_id in xrange(subscribers):
        for i in random.sample([1,2,3,4],random.randint(1,4)):
            sf_type = i
            is_active = 0 if random.random() <= 0.84 else 1
            error_cntrl = random.randrange(256)
            data_a = random.randrange(256)
            data_b = ''.join(random.choice(string.ascii_uppercase) for i in range(5))
            facility_writer.save([s_id+1, sf_type, is_active, error_cntrl, data_a, data_b])
            forwarding_entries = random.randrange(4)
            if forwarding_entries!= 0:
                generate_call_forwarding_data(s_id, i, forwarding_entries, forwarding_writer)
    facility_writer.close()
    forwarding_writer.close()

def generate_call_forwarding_data(s_id, sf_type, forwarding_entries, writer):

    start_times = [0,8,16]
    random.shuffle(start_times)
    for i in range(forwarding_entries):
        start_time = start_times.pop()
        end_time = start_time + random.randrange(8)+1
        numberx = ''.join(random.choice(string.digits) for i in range(15))
        writer.save([s_id+1, sf_type, start_time, end_time, numberx])

if __name__ == "__main__":
    import sys
    assert len(sys.argv)== 2, 'destination directory missing'
    tbldir = os.path.abspath(sys.argv[1])
    g = Generator(tbldir, 1)
    g.execute()










## CLASS
