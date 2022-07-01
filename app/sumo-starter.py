from sre_parse import State
import sys, os, subprocess
from typing import List, Dict, Any
import time

from kafka import KafkaConsumer, KafkaProducer
import traci
import traci.constants as tc

import kafka_functions as kf

# TODO: Change the path to sumo home!
os.environ["SUMO_HOME"] = "C:/Program Files (x86)/Eclipse/Sumo/"

if 'SUMO_HOME' in os.environ:
    tools = os.path.join(os.environ['SUMO_HOME'], 'tools')
    sys.path.append(tools)
else:
    sys.exit("please declare environment variable 'SUMO_HOME'")

def run(steps: int, edges: List[str], tls: List[str], producer: KafkaProducer, consumer: KafkaConsumer) -> None:
    for step in range(steps):
        traci.simulationStep()
        if step%1 == 0:
            vehicles = traci.vehicle.getIDList()
            for vehicle in vehicles:
                speed = traci.vehicle.getSpeed(vehicle)
                next_tls = traci.vehicle.getNextTLS(vehicle)
                if len(next_tls) > 0:
                    tl = next_tls[0][0]
                    tl_state = next_tls[0][-1]
                    if (next_tls[0][2] <= 30):
                        kf.kafka_publish(
                            topic = 'vehicles',
                            producer=producer,
                            value={
                                'step': float(step),
                                'veh_id': str(vehicle),
                                'speed': float(speed),
                                'next_tl': str(tl),
                                'next_tl_state': str(tl_state),
                                'ts': int(time.time())
                            }
                        )
        producer.flush()
        
        msg_pack = consumer.poll()
        for tp, messages in msg_pack.items():
            for message in messages:
                tl = message.value['tl_id']
                try:
                    traci.trafficlight.setPhaseDuration(tl, 0)
                    print(f'Traffic light {tl} state changed')
                except:
                    pass

        

def close_sumo() -> None:
    traci.close

def start_simulation(sumo_cfg: str) -> None:
    kf.create_kafka_connection()
    kafka_producer = kf.create_kafka_producer()
    consumer = kf.create_kafka_consumer('output')
    
    sumo_cmd = ["sumo-gui", "-c", sumo_cfg, "--start", "--step-length", "1"]
    traci.start(sumo_cmd)
    edge_ids = subscribe_to_edges()
    #print(len(edge_ids))
    tl_ids = subscribe_to_tls()
    #print(len(tl_ids))

    steps = 100000
    try:
        run(steps=steps, edges=edge_ids, tls=tl_ids, producer=kafka_producer, consumer=consumer)
    except KeyboardInterrupt:
        pass
    finally:
        close_sumo()

def subscribe_to_edges() -> List[str]:
    edge_ids = traci.edge.getIDList()
    for edge_id in edge_ids:
        traci.edge.subscribe(
            edge_id,
            [tc.LAST_STEP_VEHICLE_NUMBER, tc.LAST_STEP_VEHICLE_ID_LIST]
        )
    return edge_ids

def subscribe_to_vehicles():
    veh_ids = traci.vehicle.getIDList()
    for veh_id in veh_ids:
        traci.vehicle.subscribe(
            veh_id,
            [tc.LAST_STEP_MEAN_SPEED]
        )
    return veh_ids

def subscribe_to_tls():
    tl_ids = traci.trafficlight.getIDList()
    for tl_id in tl_ids:
        traci.trafficlight.subscribe(
            tl_id,
            [tc.TL_RED_YELLOW_GREEN_STATE, tc.TL_CONTROLLED_JUNCTIONS, tc.TL_NEXT_SWITCH]
        )
    return tl_ids

if __name__ == "__main__":
    
    base_path = os.path.dirname(os.path.realpath(__file__))
    SUMO_CFG = f"{base_path}/sumo/Trento_mid/osm.sumocfg" 
    start_simulation(SUMO_CFG)