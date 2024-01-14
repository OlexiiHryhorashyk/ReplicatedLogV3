import os
from aiohttp import web, ClientSession, ClientError
import asyncio
from ast import literal_eval
import requests

messages_list = {}  # total ordering by saving messages with their indexes from master
latency = int(os.getenv('LATENCY', 0))
master = os.getenv('MASTER', 'master')
node_name = os.getenv('NAME', 'node')
master_port = os.getenv('MASTER_PORT', '8000')
master_url = f'http://{master}:{master_port}'
HEARTBEAT_INTERVAL = int(os.getenv('HEARTBEAT_INTERVAL', 2))  # seconds


def update_messages():
    try:
        resp = requests.get(master_url)
        print("Updating messages from master...")
        messages = str(resp.text).split(",\n")
        if len(messages) == 1 and messages[0] == "":
            return
        for i in range(len(messages)):
            messages_list.update({i: messages[i]})
    except requests.exceptions.ConnectionError:
        print("ERROR! Update failed. No connection to the Master!")


async def send_heartbeat():
    async with ClientSession() as session:
        while True:
            await asyncio.sleep(latency)  # emulating latency of node for heartbeats
            try:
                async with session.post(f'{master_url}/heartbeat', json={"name": node_name}) as response:
                    pass
                    print("<-- Heartbeat sent to master.")  # for checking sending heartbeats
            except ClientError as e:
                print(f"Error sending heartbeat to master: {e}")
            await asyncio.sleep(HEARTBEAT_INTERVAL)


async def handle_post(request):
    content = await request.content.read()
    msg = literal_eval(content.decode('utf-8'))
    print("POST handler <- Message received:", msg['message'])
    await asyncio.sleep(latency)
    if msg['index'] in messages_list.keys():  # Message deduplication by preventing saving copies of the same message
        print("POST handler ERROR <- Message", msg['message'], "already saved!")
        return web.Response(status=400, text='Deduplication. Message already saved.')
    messages_list.update({msg['index']: msg['message']})
    message = "Message received:" + str(msg['message'])
    return web.Response(text=message)


async def handle_get(request):
    messages = []
    message_id = list(messages_list.keys())
    print("Message ids:", message_id)  # for testing of total order
    print("Messages:", list(messages_list.values()))
    for i in range(max(message_id)+1):  # total order - adding messages to output string ordered by index
        if i in message_id:  # if message missed (msg0, msg1, msg3) --> (msg0, msg1)
            messages.append(str(messages_list[i]))
        else:
            break
    str_messages = ',\n'.join(str(msg) for msg in messages)
    print("GET handler -> Listing saved messages...")
    return web.Response(text=str_messages)


app = web.Application()
app.router.add_post('/', handle_post)
app.router.add_get('/', handle_get)

if __name__ == '__main__':
    print("Server started...")
    update_messages()  # updates message log from master after restart
    loop = asyncio.new_event_loop()
    asyncio.ensure_future(loop.create_task(send_heartbeat()))
    web.run_app(app, loop=loop, host='0.0.0.0', port=8080)

