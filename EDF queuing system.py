# this software simulates the arrival and departure of vehicles (packets) following an
#  Earliest Deadline First (EDF) scheduling algorithm

import numpy
import time


class TreeMap:  # Treemap class is a custom made class that puts objects on a list and sorts them according to
    # a key related to the object
    def __init__(self):
        self.dictionary1 = {}
        self.dictionary2 = {}

    def put(self, v1, v2):  # store objects in dictionary according to the small key value
        self.dictionary2 = {}
        self.dictionary1[v1] = v2
        list_of_keys = self.dictionary1.keys()
        list_of_keys = sorted(list_of_keys)
        for keY in list_of_keys:
            self.dictionary2[keY] = self.dictionary1[keY]

    def find_lowest_key(self):
        list_of_keys = self.dictionary1.keys()
        list_of_keys = sorted(list_of_keys)
        return list_of_keys[0]

    def find_highest_key(self):
        list_of_keys = self.dictionary1.keys()
        list_of_keys = sorted(list_of_keys)
        return list_of_keys[len(list_of_keys) - 1]

    def key_set(self):  # return list of keys
        return self.dictionary2.keys()

    def get_dictionary(self):
        return self.dictionary2

    def get_length(self):
        return len(self.dictionary2)

    def get_event_and_update(self):
        first_key = self.find_lowest_key()
        sample = self.dictionary2.get(first_key)
        del self.dictionary2[first_key]
        del self.dictionary1[first_key]
        return sample

    def get_event(self, key):
        return self.dictionary2[key]

    def contains_key(self, key):  # checks if a key is in a given set of keys
        value = 0
        key_list = self.dictionary2.keys()
        for i in key_list:
            if i == key:
                value = 1
        return value

    def print_keys(self):
        return self.dictionary2.keys()


class ParaMeters:

    constantService = 0
    blockingProbability = 0
    maxArrEvents = 100000
    numberOfServers = 1
    maxQueueSize = 10000
    customerArrivalRate = 0.15
    customerServiceRate = 0.3
    mean = 24.78
    stv = 7.434
    distance = 1000  # fixed value


class StatisticalEntity:  # used to compute desired outputs
    def __init__(self):
        self.n = 0
        self.x = 0.0
        self.x2 = 0.0
        self.last = 0.0
        self.maxValue = -1000000000000
        self.minValue = 1000000000000

    def add(self, value):
        self.n += 1
        self.last = value
        self.x += value
        self.x2 += value * value

        if self.minValue > value:
            self.minValue = value
        if self.maxValue < value:
            self.maxValue = value

    def get_num_sample(self):
        return self.n

    def get_last_sample(self):
        return self.last

    def get_sum(self):
        return self.x

    def get_mean(self):
        if self.n > 0:
            return self.x / self.n
        else:
            return 0.0

    def get_variance(self):
        if self.n > 1:
            return (self.x2 - ((self.x * self.x) / self.n)) / (self.n - 1)
        else:
            return 0.0

    def get_std_dev(self):
        return numpy.sqrt(self.get_variance())

    def get_minimum(self):
        return self.minValue

    def get_maximum(self):
        return self.maxValue


class Results:
    queueingDelay = StatisticalEntity()
    systemDelay = StatisticalEntity()
    matchServerCompURs = 0
    numberExpURs = 0
    numberBlockedURs = 0


class Simulator:  # simulator class implements the arrival and departure process of every vehicle
    def __init__(self):
        self.scheduler = EventScheduler()
        self.q = Queue()
        self.s = Server()
        self.ARRIVAL = 1
        self.DEPARTURE = 0
        self.numberOfServers = ParaMeters.numberOfServers
        self.blockingProbability = ParaMeters.blockingProbability
        self.customerArrivalRate = ParaMeters.customerArrivalRate
        self.customerServiceRate = ParaMeters.customerServiceRate
        self.constantService = ParaMeters.constantService
        self.mean = ParaMeters.mean
        self.stv = ParaMeters.stv
        self.distance = ParaMeters.distance
        self.list_of_servers = []

    def launch_simulation(self):  # start simulation and create first arrival event
        for i in range(0, self.numberOfServers):
            self.list_of_servers.append(self.s)  # creates a list of server objects
        while self.scheduler.event_list_empty() != 0:
            event = self.scheduler.process_next_event()  # gets event from event list and creates next event
            self.process_event(event, self.scheduler)  # process event from event list

    def process_event(self, event, scheduler):  # process event according to its type
        if event.get_type_() == self.ARRIVAL:
            self.process_arrival_event(event, scheduler)
        else:
            if event.get_type_() == self.DEPARTURE:
                self.process_departure_event(event, scheduler)
            else:
                print("Error: ", event)
                return 0

    def process_arrival_event(self, event, scheduler):  # process the arrival event by creating a customer object and
        # sets its parameters according the event
        c = Customer()
        # random gaussian generator block {
        gaussian = numpy.random.normal(self.mean, self.stv)
        while gaussian > 50 or gaussian < 10:
            gaussian = numpy.random.normal(self.mean, self.stv)
        initial_deadline = self.distance / gaussian
        #  }
        c.set_id(event.get_num())
        c.set_arrival_time(event.get_clk())
        if self.constantService == 1:
            c.set_service_time(1.0)
        else:
            c.set_service_time(numpy.random.exponential(1 / self.customerServiceRate))
        c.set_initial_deadline(initial_deadline)
        c.set_residual_deadline(initial_deadline)
        self.q.enqueue(c, event.get_clk())
        if numpy.random.uniform() >= self.blockingProbability:
            for i in range(0, self.numberOfServers):
                if self.list_of_servers[i].is_busy() == 0:  # returns status (busy = 1, idle = 0) -> if idle
                    # takes a packet from the queue to be processed and creates a departure of the packet
                    if self.q.is_empty() != 0:  # is queue not empty -> remove first customer from queue
                        c = self.q.dequeue(event.get_clk())  # if server busy then there is no dequeue of the customer
                        c.set_begin_service_time(event.get_clk())
                        c.set_complete_service_time(event.get_clk() + c.get_service_time())
                        scheduler.create_departure_event(c.get_id(), c.get_complete_service_time())
                        self.list_of_servers[i].serve(c)  # serves customer and sets server status to busy

    def process_departure_event(self, event, scheduler):
        block_prob = numpy.random.uniform()
        for i in range(0, self.numberOfServers):
            if self.list_of_servers[i].is_busy() and self.list_of_servers[i].c.get_id() == event.get_num():
                self.list_of_servers[i].discard_successful_customer(event.get_clk())  # server is set to idle
                if block_prob >= self.blockingProbability:
                    if self.q.is_empty() != 0:  # if queue not empty -> create departure and serve customer
                        c = self.q.dequeue(event.get_clk())
                        c.set_begin_service_time(event.get_clk())
                        c.set_complete_service_time(event.get_clk() + c.get_service_time())
                        scheduler.create_departure_event(c.get_id(), c.get_complete_service_time())
                        self.list_of_servers[i].serve(c)  # serves customer and sets server to busy


class Customer:  # customer object and it parameters
    def __init__(self):
        self.id = 0
        self.arrival_time = 0
        self.service_time = 0
        self.initial_deadline = 0
        self.residual_deadline = 0
        self.begin_service_time = 0
        self.complete_service_time = 0
        self.queueing_delay = 0
        self.system_delay = 0

    def set_id(self, idd):
        self.id = idd

    def get_id(self):
        return self.id

    def set_arrival_time(self, arr_time):
        self.arrival_time = arr_time

    def get_arrival_time(self):
        return self.arrival_time

    def set_service_time(self, ser_time):
        self.service_time = ser_time

    def get_service_time(self):
        return self.service_time

    def set_initial_deadline(self, initial_dead):
        self.initial_deadline = initial_dead

    def get_initial_deadline(self):
        return self.initial_deadline

    def set_residual_deadline(self, res_dead):
        self.residual_deadline = res_dead

    def get_residual_deadline(self):
        return self.residual_deadline

    def set_begin_service_time(self, begin_ser):
        self.begin_service_time = begin_ser

    def get_begin_service_time(self):
        return self.begin_service_time

    def set_complete_service_time(self, complete_ser):
        self.complete_service_time = complete_ser

    def get_complete_service_time(self):
        return self.complete_service_time

    def set_queueing_delay(self, queue_delay):
        self.queueing_delay = queue_delay

    def get_queueing_delay(self):
        return self.queueing_delay

    def set_system_delay(self, sys_delay):
        self.system_delay = sys_delay

    def get_system_delay(self):
        return self.system_delay

    def to_string(self):
        return "Customer - ID: ", self.id, " Arrival Time: ", self.arrival_time


class Queue:  # this class implements a list with customer objects that can be enqueued and dequeued
    def __init__(self):
        self.q = TreeMap()
        self.time_last_update = 0.0
        self.queue_size = 0
        self.max_queue_size = ParaMeters.maxQueueSize

    def update_residual_deadlines(self, current_t):  # updates deadlines of all packets every time a customer arrives
        if self.q .get_length() == 0:
            self.time_last_update = current_t
            return
        decrement = current_t - self.time_last_update
        key_set = self.q.key_set()
        temporary_q = TreeMap()
        for key in key_set:
            customer_list = self.q.get_event(key)
            residual_deadline = key - decrement
            if residual_deadline <= 0:
                if key > 0:
                    Results.numberExpURs += len(customer_list)
                temporary_q.put(0.0, customer_list)
            else:
                temporary_q.put(residual_deadline, customer_list)
        self.time_last_update = current_t
        self.q = None
        self.q = temporary_q

    def enqueue(self, c, current_t):  # puts packet in queue according to its deadline
        customer_list = list()
        self.update_residual_deadlines(current_t)
        customer_key = c.get_residual_deadline()
        if self.queue_size < self.max_queue_size:
            customer_list.append(c)
            self.q.put(customer_key, customer_list)
            self.queue_size += 1
        else:
            c = None
            Results.numberBlockedURs += 1

    def dequeue(self, current_t):  # removes first packet from queue
        c = Customer()
        if self.q.get_length() == 0:
            return None
        else:
            first_key = self.q.find_lowest_key()
            customer_list = self.q.get_event(first_key)
            if len(customer_list) > 0:
                c = customer_list.pop(0)
            if len(customer_list) == 0:
                self.q.get_event_and_update()
            c.set_queueing_delay(current_t - c.get_arrival_time())
            Results.queueingDelay.add(c.get_queueing_delay())
            if first_key > 0:
                Results.matchServerCompURs += 1
            self.queue_size -= 1
            return c

    def is_empty(self):
        return self.q.get_length()


class Server:  # server object
    def __init__(self):
        self.num = 0
        self.BUSY = 1
        self.IDLE = 0
        self.c = Customer()
        self.status = self.IDLE
        self.c = None

    def is_busy(self):  # check server status
        return self.status

    def discard_successful_customer(self, success_comp_time):  # remove customer from queue after it has been served
        Results.systemDelay.add(success_comp_time - self.c.get_arrival_time())
        self.status = self.IDLE
        self.c = None

    def serve(self, c):  # start serving process
        self.num += 1
        self.c = c
        self.status = self.BUSY

    def get_customer(self):  # get customer object in server
        return self.c


class Event:  # event object and its methods
    ARRIVAL = 1
    DEPARTURE = 0

    def __init__(self, num, type_, clk):
        self.num = num
        self.type_ = type_
        self.clk = clk

    def precedes(self, evt):
        return self.clk < evt.clk

    def get_num(self):
        return self.num

    def set_num(self, num):
        self.num = num

    def get_clk(self):
        return self.clk

    def set_clk(self, clk):
        self.clk = clk

    def get_type_(self):
        return self.type_

    def set_type_(self, type_):
        self.type_ = type_

    def equal(self, evt):
        equivalence = False
        if evt.get_num() == self.num and evt.get_clk() == self.clk and evt.get_type_() == self.type_:
            equivalence = True
        return equivalence


class EventScheduler:  # event scheduler creates events and adds and removed them from the event list
    def __init__(self):
        self.count1 = 0
        self.count2 = 0
        self.event_list = TreeMap()
        self.master_clock = 0.0
        self.number_arrivals_events = 0
        self.ARRIVAL = 1
        self.DEPARTURE = 0
        self.max_arrivals_events = ParaMeters.maxArrEvents
        self.customer_arrivals_rate = ParaMeters.customerArrivalRate
        self.customer_service_rate = ParaMeters.customerServiceRate
        self.initialize_scheduler()

    def initialize_scheduler(self):
        rnd_uniform = 0
        while rnd_uniform == 0:
            rnd_uniform = numpy.random.uniform()
        initial_arrival_clock = numpy.random.exponential(1 / self.customer_arrivals_rate)
        self.event_list.put(initial_arrival_clock,
                            Event(++self.number_arrivals_events, self.ARRIVAL, initial_arrival_clock))

    def process_next_event(self):
        if self.event_list.get_length() == 0:
            return None
        next_event = self.event_list.get_event_and_update()
        if next_event.get_type_() == self.ARRIVAL:
            if self.number_arrivals_events < self.max_arrivals_events:
                self.number_arrivals_events += 1
                self.create_arrival_event(next_event)
        return next_event

    def create_arrival_event(self, last_arrival_event):
        self.count1 += 1
        rnd_uniform = 0
        while rnd_uniform == 0:
            rnd_uniform = numpy.random.uniform()
        delta_t = numpy.random.exponential(1 / self.customer_arrivals_rate)
        arrival_instant = last_arrival_event.get_clk() + delta_t
        self.event_list.put(arrival_instant, Event(last_arrival_event.get_num() + 1, self.ARRIVAL, arrival_instant))

    def create_departure_event(self, departing_cust_id, departure_instant):
        self.count2 += 1
        self.event_list.put(departure_instant, Event(departing_cust_id, self.DEPARTURE, departure_instant))

    def event_list_empty(self):
        if self.event_list.get_length() == 0:
            return False
        else:
            return True

    def number_of_events(self):
        return self.count1, self.count2


# Main
start = time.time()
sim = Simulator()
sim.launch_simulation()
end = time.time()
print("Execution time= :", (end - start))
print("Probability of matched service completion: ", (Results.matchServerCompURs / ParaMeters.maxArrEvents))
print("Probability of deadline mismatch: ", (Results.numberExpURs / ParaMeters.maxArrEvents))
print("Probability of blocking: ", (Results.numberBlockedURs / ParaMeters.maxArrEvents))
print("System response time: ", Results.systemDelay.get_mean())
print("Queuing response time: ", Results.queueingDelay.get_mean())
