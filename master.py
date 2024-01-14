import os
import aiohttp
import asyncio
from aiohttp import web
from ast import literal_eval
from aiohttp_retry import RetryClient, ExponentialRetry
import datetime
from count_down_latch import CountDownLatch

NODE_NAMES = os.getenv('NODE_NAMES', 'node1 node2')
HEARTBEAT_INTERVAL = int(os.getenv('HEARTBEAT_INTERVAL', 2))  # seconds
nodes = list(NODE_NAMES.split())
messages_list = {}
url = [f'http://{node}:8080' for node in nodes]  # for Docker containers
message_index = 0
last_heartbeat_time = [datetime.datetime.now() for node in nodes]
nodes_alive = [False for node in nodes]  # status dead/alive of nodes updated by heartbeats (for quorum)
health_status = ['Healthy' for node in nodes]


async def send_to_sub(url_address: str, msg: dict, latch: CountDownLatch):
    global message_index
    node_status = health_status[url.index(url_address)]
    if node_status == "Healthy":  # timeout increasing if node status is getting worse
        wait = 3
    elif node_status == "Suspected":
        wait = 5
    else:
        wait = 10
    print("Hello!")
    timeout = aiohttp.ClientTimeout(total=wait)  # set client timeout. Bigger timeout -> more retries
    # retry_options = ExponentialRetry(attempts=20)  # we can identify max number of retries
    retry_options = ExponentialRetry()  # retry delay grows exponentially with each attempt till timeout
    try:
        # To implement retry functionality we use aiohttp_retry client
        async with RetryClient(retry_options=retry_options, timeout=timeout) as session:
            async with session.post(url_address, json=msg):
                print(f"POST request -> Message sent to {url.index(url_address) + 1} subsequent server - {msg['message']}")
        await latch.count_down()
        return True
    except aiohttp.ClientError as error:
        print(error)
        print(f"POST ERROR -> No connection to the {url.index(url_address) + 1} sub server! Message not passed!")
        await latch.count_down()
        return False


async def handle_post(request):
    global nodes_alive  # we check majority by node status from heartbeats
    alive = sum(1 for alive in nodes_alive if alive)  # count number of alive nodes
    if 1+alive < len(nodes_alive)-alive:  # if no majority available
        return web.Response(text=f"Message cant be saved! Quorum is dead!")
    content = await request.content.read()
    msg = literal_eval(content.decode('utf-8'))
    global message_index
    print("POST handler <- Message received:", msg)
    message = msg.get('message', '')
    msg_str = message
    if message.startswith("w="):
        try:
            w = int(message[2])
        except ValueError:
            w = 3
            print(f"ERROR! Cant set write concern! w={message[2]} is not a number!")
        msg_str = message[3:]
    else:
        w = len(url)+1
    messages_list.update({message_index: msg_str})  # Saving as a dictionary to mach the type on secondary
    msg = {
        'message': msg_str,
        'index': message_index
    }
    if w > len(nodes)+1:
        w = len(nodes)+1
    print("w=", w)
    message_index += 1
    latch = CountDownLatch(count=w-1 if w > 0 else 0)
    tasks = [asyncio.create_task(send_to_sub(url_address, msg, latch)) for url_address in url]
    for task in tasks:
        asyncio.ensure_future(task)
    await latch.tasks_await()  # Waiting for w tasks
    if 1 + alive < w:  # if no w (w=3) number of nodes alive
        print(f"ERROR! Cant replicate message. No {w} nodes alive.")
        # return web.Response(text=f"Message cant be saved! No {w} nodes alive.!")  # how would i do it
        # I would strongly recommend to remove this while loop. We have our retry/timeout which works just fine.
        while len([True for node in nodes_alive if node]) < w-1:  # blocks client until w nodes alive (requirement of task)
            await asyncio.sleep(1)  # client will wait to eternity if any node will stay dead
    results = [task.result() for task in tasks if task.done()]
    print(results)
    if False in results or len(results) < w-1:  # if any node is non-responsive, informs a client.
        return web.Response(text=f"Cant replicate {msg_str} to {w} nodes! Timeout reached.")
    return web.Response(text=f"Message received: {msg_str}")


async def handle_get(request):
    str_messages = ',\n'.join(str(msg) for msg in messages_list.values())
    print("GET handler -> Listing saved messages...")
    return web.Response(text=str_messages)


async def handle_heartbeat(request):
    content = await request.content.read()
    msg = literal_eval(content.decode('utf-8'))
    name = str(msg.get('name', ''))
    global last_heartbeat_time
    global nodes
    if name in nodes:
        last_time = last_heartbeat_time[nodes.index(name)]
        now = datetime.datetime.now()
        time_since_last = (now - last_time).total_seconds()
        if not nodes_alive[nodes.index(name)]:
            nodes_alive[nodes.index(name)] = True
        if time_since_last > HEARTBEAT_INTERVAL * 2:
            health_status[nodes.index(name)] = "Unhealthy"
        else:
            if time_since_last > HEARTBEAT_INTERVAL + 1:  # if delay > 1sec - suspected
                health_status[nodes.index(name)] = "Suspected"
            else:
                health_status[nodes.index(name)] = "Healthy"

        last_heartbeat_time[nodes.index(name)] = datetime.datetime.now()
        print(f"--> Heartbeat received from {name} - time since last {time_since_last}sec.")
    return web.Response(text="Heartbeat received.")


async def check_heartbeat():  # Checking that nodes are alive
    global last_heartbeat_time
    while True:
        print("Nodes status:", health_status)
        for last_time in last_heartbeat_time:
            node = nodes[last_heartbeat_time.index(last_time)]
            now = datetime.datetime.now()
            time_since_last = (now - last_time).total_seconds()
            if time_since_last > HEARTBEAT_INTERVAL * 3:  # if timeout is more than three heartbeats node is dead
                print(f"ERROR! No heartbeat received from {node}. Connection lost.")
                health_status[nodes.index(node)] = "Dead"
                nodes_alive[nodes.index(node)] = False
        await asyncio.sleep(round(HEARTBEAT_INTERVAL))


async def handle_health_report(request):
    master_status = "Master: Alive\n"
    str_messages = master_status+',\n'.join(f"Node: {nodes[i]} --> {health_status[i]} - Alive:{nodes_alive[i]}" for i in range(len(nodes)))
    print("GET health --> Node health status report...")
    return web.Response(text=str_messages)

app = web.Application()
app.router.add_post('/', handle_post)
app.router.add_get('/', handle_get)
app.router.add_post('/heartbeat', handle_heartbeat)
app.router.add_get('/health', handle_health_report)

if __name__ == '__main__':
    print("Server started...")
    loop = asyncio.new_event_loop()
    asyncio.ensure_future(loop.create_task(check_heartbeat()))
    web.run_app(app, loop=loop, host='0.0.0.0', port=8000)
