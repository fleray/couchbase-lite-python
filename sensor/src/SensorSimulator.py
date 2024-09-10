import random, time

def generate_json_doc(last_value, sensor_id):
    return {'type': 'sensor', 'timestamp': time.time(), 'temperature': generate_value(last_value), 'sensor': sensor_id}

def generate_value(last_value):
    new_value = 0.
    if last_value is None:
        return  random.uniform(10, 40)* 40 - 10
    else:
        new_value = last_value + random.uniform(1,3) * 2 - 1

    if new_value > 50:
        return last_value

    if new_value < -20:
        return last_value

    return new_value

