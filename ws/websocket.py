import asyncio
import json
import websockets
import nest_asyncio
import logging
import time

from config import conn

STATE = {'channels': []}

async def alpaca_conn(websocket, path, channels):
    """
    Connect to alpaca data websocket. Listen to channels.
    """
    @conn.on(r'^AM\..+$')
    async def on_minute_bars(conn, channel, bar):
        """
        Alpaca websocket event handler. Send data to client.
        """
        data = bar.__dict__['_raw']
        print('< (alpaca): ', data)

        json_data = {
            'message': {
                'data': data
            }
        } 
        await websocket.send(json.dumps(json_data))

    try:
        print('Alpaca connection ', conn)
        await conn.run(channels)
    except Exception as e:
        print(f'Exception from websocket connection: {e}')
    finally:
        print("Trying to re-establish connection")
        time.sleep(5)
        await alpaca_conn(websocket, path, channels)
        
async def consumer(websocket, path, message):
    """
    Parse message. Add channels to state.
    """
    jsonMessage = json.loads(message)
    print('< (consumer): ', jsonMessage)

    if 'subscribe' in  jsonMessage['message']:
        channels = jsonMessage['message']['subscribe']

        # add channels to state, check if channel exists
        for channel in channels:
            if channel in STATE['channels']:
                print(f'{channel} already in STATE[\'channels\']')
            else:
                STATE['channels'].append(channel)
        print('STATE[\'channels\'] = ', STATE['channels'])

    elif 'unsubscribe' in  jsonMessage['message']:
        channels = jsonMessage['message']['unsubscribe']

        # remove channels from STATE
        for channel in channels:
            if channel in STATE['channels']:
                STATE['channels'].remove(channel)
                print(f'removed {channel} => ', STATE['channels'])

    # return current state to client
    subscriptions = {
        'message': {
            'subscriptions': STATE['channels']
        }
    }
    await websocket.send(json.dumps(subscriptions))

    await alpaca_conn(websocket, path, STATE['channels'])


async def consumer_handler(websocket, path):
    """
    Loop to process incoming messages from clients.
    Message is a list of channels.

    :type message: str(list[str])
    """
    print('A client has connected!')

    async for message in websocket:
        await consumer(websocket, path, message)


if __name__ == '__main__':
    nest_asyncio.apply()
    
    logger = logging.getLogger('websockets')
    logger.setLevel(logging.INFO)
    logger.addHandler(logging.StreamHandler())
    logging.basicConfig(format='%(asctime)s %(message)s', level=logging.INFO)

    # TODO: add auth protocol, websocket headers
    # websockets.auth.basic_auth_protocol_factory(
    #     realm,
    #     credentials=None, 
    #     check_credentials=None,
    # )

    print('Starting server! Waiting for consumers...')
    start_server = websockets.serve(consumer_handler, "localhost", 8765)

    asyncio.get_event_loop().run_until_complete(start_server)
    asyncio.get_event_loop().run_forever()