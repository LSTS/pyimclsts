import src.pyimclsts.network as n
import pyimc_generated as pg
import random 

class FollowRef_Vehicle():
    '''
    Minimal implementation to start a Follow Reference manuever
    '''
    __slots__ = ['EstimatedState', 'FollowRefState', 'peers', 'target', 'request_id', 'in_ip', 'in_port']
    
    def __init__(self, target : str, in_ip : str = "127.0.0.1", in_port : int = 8000):

        '''target is the name of the vehicle as in Announce messages'''
        self.EstimatedState = None
        self.FollowRefState = None
        self.in_ip = in_ip
        self.in_port = in_port
        self.target = target
        self.peers = dict()

    def send_announce(self, send_callback):
        
        announce = pg.messages.Announce()
        announce.sys_name = 'python-client'
        announce.sys_type = 0
        announce.owner = 65535
        announce.lat = 0.7186986607
        announce.lon = -0.150025012
        announce.height = 0
        announce.services = 'imc+udp://' + self.in_ip + ':' + str(self.in_port)
        
        send_callback(announce, dst = self.peers.get(self.target, 0xFFFF))

    def update_vehicle_state(self, msg : pg.messages.EstimatedState, send_callback):
        print(msg)
        self.EstimatedState = msg

    def update_plan_state(self, msg : pg.messages.FollowRefState, send_callback):
        self.FollowRefState = msg
    

if __name__ == '__main__':
    con = n.udp_interface('localhost', 8000, 'localhost', 6002)
    sub = n.subscriber(con)

    # This is just an object to keep track of all the info related with the vehicle. 
    vehicle = FollowRef_Vehicle('lauv-xplore-1', 'localhost', 8000)

    # Set a delay, so that we receive the Announcements
    #sub.call_once(vehicle.request_followRef, 5)
    
    sub.subscribe_async(vehicle.update_vehicle_state, pg.messages.EstimatedState)
    sub.periodic_async(vehicle.send_announce, 10)

    sub.run()
    


