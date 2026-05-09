from kafka import KafkaConsumer
from collections import Counter, defaultdict
import json
from datetime import datetime

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    auto_offset_reset='earliest',
    group_id='anomalia-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

transaction_history = defaultdict(list)

print("nasłuchuję czy wystepują >3 transakcje w ciagu minuty")


for message in consumer:
    tx =message.value 
    user_id = tx['user_id']
    tx_time = datetime.fromisoformat(tx['timestamp'])
    transaction_history[user_id].append(tx_time)
    ostatnie_transactions = []

    for miniony_czas in transaction_history[user_id]:
        roznica_czasu = (tx_time - miniony_czas).total_seconds()
        if roznica_czasu <=60:
            ostatnie_transactions.append(miniony_czas)
            
    transaction_history[user_id] = ostatnie_transactions

    if len(transaction_history[user_id]) > 3:
        print(
            f'ALERT użytkownik {user_id} dokonał więcej niż 3 płatności w ciagu 1 minuty !!!'
        )
    

