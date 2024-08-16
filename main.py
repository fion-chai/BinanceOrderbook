import websocket
import requests
from datetime import datetime
from json import loads
import pandas as pd
import numpy as np

class Stream():
    def __init__(self):
        self.orderbook = {}
        self.prev_u = -1
        self.symbol = ''

    def get_snapshot(self):
        url = 'https://api.binance.com'

        api_call = f'/api/v3/depth?symbol={self.symbol.upper()}&limit=1000'

        response = requests.get(url + api_call)
        self.orderbook = loads(response.content.decode())

    def update_orderbook(self, data):
        if len(data['b']) != 0:
            update_bid = data['b']
            update_bid = np.array(update_bid, dtype=float)
            update_bid = pd.DataFrame(update_bid, columns=['Price', 'Quantity'])

            bid = self.orderbook['bids']

            bid = bid.merge(update_bid, on='Price', how='outer').sort_values(by=['Price'], ascending=False)
            bid['Quantity'] = bid.apply(lambda row: row['Quantity_y'] if pd.notnull(row['Quantity_y']) else row['Quantity_x'], axis=1)

            bid = bid.drop(bid[bid['Quantity'] == 0].index).reset_index(drop=True)
            
            self.orderbook['bids'] = bid[['Price', 'Quantity']]
    
        if len(data['a']) != 0:
            update_ask = data['a']
            update_ask = np.array(update_ask, dtype=float)
            update_ask = pd.DataFrame(update_ask, columns=['Price', 'Quantity'])

            ask = self.orderbook['asks']

            ask = ask.merge(update_ask, on='Price', how='outer').sort_values(by=['Price'])
            ask['Quantity'] = ask.apply(lambda row: row['Quantity_y'] if pd.notnull(row['Quantity_y']) else row['Quantity_x'], axis=1)

            ask = ask.drop(ask[ask['Quantity'] == 0].index).reset_index(drop=True)

            self.orderbook['asks'] = ask[['Price', 'Quantity']]

        print(f"UPDATE {datetime.now()}")

        print("Bid:")
        print(self.orderbook['bids'][:5])

        print("Ask:")
        print(self.orderbook['asks'][:5])

        print()

    def on_message(self, ws, message):
        data = loads(message)
        
        if len(self.orderbook) == 0:
            self.get_snapshot()
            bid = self.orderbook['bids']

            bid = np.array(bid, dtype=float)
            bid = pd.DataFrame(bid, columns=['Price', 'Quantity'])

            ask = self.orderbook['asks']
            ask = np.array(ask, dtype=float)
            ask = pd.DataFrame(ask, columns=['Price', 'Quantity'])

            self.orderbook['bids'] = bid
            self.orderbook['asks'] = ask
        
        lastUpdateId = self.orderbook['lastUpdateId']

        # first update
        if data['u'] > lastUpdateId and self.prev_u == -1:
            if data['U'] <= lastUpdateId+1 and data['u'] >= lastUpdateId+1:
                self.update_orderbook(data)
                self.prev_u = data['u']
                
        elif data['U'] == self.prev_u + 1:
            self.update_orderbook(data)
            self.prev_u = data['u']
            
        else:
            print("Discard")

    def on_error(self, ws, error):
        print(error)

    def on_close(self, ws, close_status_code, close_message):
        print("Closed")

    def on_open(self, ws):
        print('Connected')
        print()

    def connect_stream(self, user_input):
        self.symbol = user_input
        socket = f'wss://stream.binance.com:9443/ws/{user_input}@depth'  

        ws = websocket.WebSocketApp(socket, 
                                    on_open=self.on_open,
                                    on_close=self.on_close,
                                    on_error=self.on_error,
                                    on_message=self.on_message)

        ws.run_forever()

def main():
    user_input = input("Enter a symbol: ").lower()
    stream = Stream()
    stream.connect_stream(user_input)

main()