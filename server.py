import asyncio
import codecs
from conf import conf_dict
import redis
import time


class Server:

    def __init__(self):
        self.redis_host = conf_dict.get("redis").get("host")
        self.redis_port = conf_dict.get("redis").get("port")
        self.redis_db = conf_dict.get("redis").get("db")
        self.r = redis.StrictRedis(host=self.redis_host,
                                   port=self.redis_port, db=self.redis_db)
        # conditional for not resetting count if it exists
        # if not self.r.get("count"):
        self.r.set('count', 0)
        # if not self.r.get("start_time"):
        millis = self.get_time()
        self.r.set('start_time', millis)
        self.r.delete("time_series")
        self.loop = asyncio.get_event_loop()
        self.host = conf_dict.get("server_a").get("host")
        self.port = conf_dict.get("server_a").get("port")

        """  Start a socket server, call back for each client connected."""
        print("Starting Server on host: {} port: {}".format(self.host, self.port))
        self.coro = asyncio.start_server(self.handle_requests, self.host, self.port, loop=self.loop)
        tasks = [self.coro, self.calc_request_avg(self.loop)]

        """  Run the event loop until a Future is done."""
        self.server = self.loop.run_until_complete(asyncio.gather(*tasks))
        self.run()

    def get_time(self):
        return int(round(time.time() * 1000))

    '''
    Calculate the current average number of requests per second,
    append to the list to create a time series which could be plotted
    '''
    async def calc_request_avg(self, loop):
        while True:
            number_requests = int(self.r.get('count'))
            current_time = self.get_time()
            start_time = int(self.r.get("start_time"))
            request_persec = number_requests / (current_time - start_time)
            print(request_persec*1000)
            self.r.append("time_series", int(request_persec))
            await asyncio.sleep(1)

    '''
    Accept the request, increment the request counter
    '''
    async def handle_requests(self, reader, writer):
        self.r.incr('count')
        print(self.r.get('count'))
        utf8_decoder = codecs.getincrementaldecoder('utf-8')
        decoder = utf8_decoder()
        data = await reader.read(100)
        message = decoder.decode(data)
        addr = writer.get_extra_info('peername')
        print("Message {} from {}".format(message, addr))

        writer.write(data)
        await writer.drain()
        # Close socket connection
        writer.close()

    def close(self, msg):
        # close the server
        print("Ending the server due to reason: {}".format(msg))
        self.server.close()
        self.loop.run_until_complete(self.server.wait_closed())
        self.loop.close()

    def run(self):
        try:
            # run tasks
            self.loop.run_forever()
        except KeyboardInterrupt as e:
            self.close(e)

if __name__ == '__main__':
    server = Server()
