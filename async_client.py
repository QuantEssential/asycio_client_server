import aiohttp
import asyncio
import async_timeout
from conf import conf_dict
import sys
import time


class Client:

    def __init__(self, msg="", host=None, port=None, rps=10):
        if not host or not port:
            raise ConnectionError("Host or Port missing")
        self.message = msg
        self.host = host
        self.port = port
        self.paths = [(host, port), ]
        self.requests_per_second = rps
        self.client_sleep = 0.001
        # time to sleep in order to send exactly n requests per second
        self.toggle_sleep = 1 - self.client_sleep * self.requests_per_second
        # Return an asyncio event loop.
        self.loop = asyncio.get_event_loop()
        self.run()

    def run(self):
        # Run the event loop until a Future is done.
        self.loop.run_until_complete(self.session_loop(self.loop))

    async def session_loop(self, loop):
        async with aiohttp.ClientSession(loop=loop) as sess:
            while True:
                try:
                    tasks = [self.send_requests(sess, server_host, server_port, self.requests_per_second)
                             for (server_host, server_port) in self.paths]
                    await asyncio.gather(*tasks)
                    # in order to send exactly n requests per second, sleep after all requests are sent
                    await asyncio.sleep(self.toggle_sleep)
                except Exception as exc:
                    print(exc)

    async def send_requests(self, sess, server_host, server_port, req_per_sec):
        with async_timeout.timeout(1):
            print('Sending {} requests to host: {} port: {}'.format(
                req_per_sec, server_host, server_port))
            for i in range(0, req_per_sec):
                '''A wrapper for create_connection() returning a (reader, writer) pair.
                The reader returned is a StreamReader instance; the writer is a
                StreamWriter instance.'''
                reader, writer = await asyncio.open_connection(
                    server_host, server_port, loop=self.loop)
                print('Sending {}'.format(self.message))
                writer.write(self.message.encode())

                data = await reader.read(256)
                response = data.decode()
                print('Received: {}, Ending Connection'.format(response))
                writer.close()
                time.sleep(self.client_sleep)  # be nice to the system :)

if __name__ == '__main__':
    try:
        host = conf_dict.get("client_a").get("host")
        port = conf_dict.get("client_a").get("port")
        rps = conf_dict.get("client_a").get("requests_per_sec")
        msg = "Client Instance A"
        new_client = Client(msg=msg, host=host, port=port, rps=rps)
    except Exception as e:
        raise e.with_traceback(sys.exc_info()[2])
