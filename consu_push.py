from confluent_kafka import Consumer, KafkaException, KafkaError
import sys
import pymongo

# 用來接收從Consumer instance發出的error訊息
def error_cb(err):
    print('Error: %s' % err)


# 轉換msgKey或msgValue成為utf-8的字串
def try_decode_utf8(data):
    if data:
        return data.decode('utf-8')
    else:
        return None


# 指定要從哪個partition, offset開始讀資料
# def my_assign(consumer_instance, partitions):
#     for p in partitions:
#         p.offset = 0
#     print('assign', partitions)
#     consumer_instance.assign(partitions)


def print_sync_commit_result(partitions):
    if partitions is None:
        print('# Failed to commit offsets')
    else:
        for p in partitions:
            print('# Committed offsets for: %s-%s {offset=%s}' % (p.topic, p.partition, p.offset))
def memberin2():
    props = {
        'bootstrap.servers': '10.1.1.133:9092',  # Kafka集群
        'group.id': 'peter',  # ConsumerGroup的名稱
        'auto.offset.reset': 'latest',
        'enable.auto.commit': False,
        'error_cb': error_cb  # 設定接收error訊息的callback函數
    }

    # 步驟2. 產生一個Kafka的Consumer的實例
    consumer = Consumer(props)
    # 步驟3. 指定想要訂閱訊息的topic名稱
    topicName = 'logs'
    # 步驟4. 讓Consumer向Kafka集群訂閱指定的topic
    consumer.subscribe([topicName])

    # 步驟5. 持續的拉取Kafka有進來的訊息
    try:
        while True:
            records_pulled = False  # 用來檢查是否有有效的record被取出來

            # 請求Kafka把新的訊息吐出來
            records = consumer.consume(num_messages=500, timeout=1.0)  # 批次讀取
            if records is None:
                continue

            for record in records:
                # 檢查是否有錯誤
                if record is None:
                    continue
                if record.error():
                    # Error or event
                    if record.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition event
                        sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                         (record.topic(), record.partition(), record.offset()))
                    else:
                        # Error
                        raise KafkaException(record.error())
                else:
                    records_pulled = True

                    # ** 在這裡進行商業邏輯與訊息處理 **
                    # 取出相關的metadata
                    # topic = record.topic()
                    # partition = record.partition()
                    # offset = record.offset()
                    # timestamp = record.timestamp()
                    # 取出msgKey與msgValue
                    msgKey = try_decode_utf8(record.key())
                    msgValue = try_decode_utf8(record.value())
                    sendmsg = {
                        msgKey: msgValue
                    }
                    qq= {}
                    # 秀出metadata與msgKey & msgValue訊息
                    # print('%s-%d-%d : (%s , %s)' % (topic, partition, offset, msgKey, msgValue))
                    # test_msg = ('%s : %s' % (msgKey,msgValue))
                    print(sendmsg)
                    print ("已偵測到有會員進入商店")
                    a = sendmsg['login'].split("'")
                    qq.setdefault("Name",a[5])
                    print(qq)
                    client = pymongo.MongoClient(
                        "mongodb+srv://peter:0987602620@cluster0.0qqo9.mongodb.net/ceb101?retryWrites=true&w=majority")

                    mydb = client.wow
                    mycol = mydb['fit']
                    mycol.insert_many([qq])

                    consumer.close()

            # 同步地執行commit (Sync commit)
            # if records_pulled:
            #     offsets = consumer.commit(asynchronous=False)
            #     print_sync_commit_result(offsets)
    except KeyboardInterrupt as e:
        sys.stderr.write('Aborted by user\n')
    except Exception as e:
        sys.stderr.write(str(e))
    finally:
        print("慢慢逛")
def memberin():
    props = {
        'bootstrap.servers': '10.1.1.133:9092',  # Kafka集群在那裡? (置換成要連接的Kafka集群)
        'group.id': 'peter',  # ConsumerGroup的名稱 (置換成你/妳的學員ID)
        'auto.offset.reset': 'latest',  # 是否從這個ConsumerGroup尚未讀取的partition/offset開始讀earliest
        'enable.auto.commit': False,  # 是否啟動自動commit
        'error_cb': error_cb  # 設定接收error訊息的callback函數
    }

    # 步驟2. 產生一個Kafka的Consumer的實例
    consumer = Consumer(props)
    # 步驟3. 指定想要訂閱訊息的topic名稱
    topicName = 'logs'
    # 步驟4. 讓Consumer向Kafka集群訂閱指定的topic
    consumer.subscribe([topicName])

    # 步驟5. 持續的拉取Kafka有進來的訊息
    try:
        while True:
            records_pulled = False  # 用來檢查是否有有效的record被取出來

            # 請求Kafka把新的訊息吐出來
            records = consumer.consume(num_messages=500, timeout=1.0)  # 批次讀取
            if records is None:
                continue

            for record in records:
                # 檢查是否有錯誤
                if record is None:
                    continue
                if record.error():
                    # Error or event
                    if record.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition event
                        sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                         (record.topic(), record.partition(), record.offset()))
                    else:
                        # Error
                        raise KafkaException(record.error())
                else:
                    records_pulled = True

                    # ** 在這裡進行商業邏輯與訊息處理 **
                    # 取出相關的metadata
                    # topic = record.topic()
                    # partition = record.partition()
                    # offset = record.offset()
                    # timestamp = record.timestamp()
                    # 取出msgKey與msgValue
                    msgKey = try_decode_utf8(record.key())
                    msgValue = try_decode_utf8(record.value())
                    sendmsg = {
                        msgKey: msgValue
                    }
                    qq2 = {}
                    # 秀出metadata與msgKey & msgValue訊息
                    # print('%s-%d-%d : (%s , %s)' % (topic, partition, offset, msgKey, msgValue))
                    # test_msg = ('%s : %s' % (msgKey,msgValue))
                    print(sendmsg)
                    print("已偵測到有會員進入商店")
                    # a = sendmsg['login'].split("'")
                    # qq2.setdefault("Name", a[5])
                    # # print(qq2)
                    # client = pymongo.MongoClient(
                    #     "mongodb+srv://peter:0987602620@cluster0.0qqo9.mongodb.net/ceb101?retryWrites=true&w=majority")
                    #
                    # mydb = client.wow
                    # mycol = mydb['fit']
                    # mycol.insert_many([qq2])

                    consumer.close()
            # 同步地執行commit (Sync commit)
            # if records_pulled:
            #     offsets = consumer.commit(asynchronous=False)
            #     print_sync_commit_result(offsets)
    except KeyboardInterrupt as e:
        sys.stderr.write('Aborted by user\n')
    except Exception as e:
        sys.stderr.write(str(e))

    finally:

        print("慢慢逛哦")
        # 步驟6.關掉Consumer實例的連線
        # consumer.close()
def get_items():
    props = {
        'bootstrap.servers': '10.1.1.133:9092',
        'group.id': 'peter',
        'auto.offset.reset': 'latest',
        'enable.auto.commit': True,
        'error_cb': error_cb
    }
    # 步驟2. 產生一個Kafka的Consumer的實例
    consumer = Consumer(props)
    # 步驟3. 指定想要訂閱訊息的topic名稱
    topicName = 'items'
    # 步驟4. 讓Consumer向Kafka集群訂閱指定的topic
    consumer.subscribe([topicName])

    # 步驟5. 持續的拉取Kafka有進來的訊息
    try:
        while True:
            records_pulled = False  # 用來檢查是否有有效的record被取出來

            # 請求Kafka把新的訊息吐出來
            records = consumer.consume(num_messages=500, timeout=1.0)  # 批次讀取
            if records is None:
                continue

            for record in records:
                # 檢查是否有錯誤
                if record is None:
                    continue
                if record.error():
                    # Error or event
                    if record.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition event
                        sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                         (record.topic(), record.partition(), record.offset()))
                    else:
                        # Error
                        raise KafkaException(record.error())
                else:
                    records_pulled = True

                    # ** 在這裡進行商業邏輯與訊息處理 **
                    # 取出相關的metadata
                    # topic = record.topic()
                    # partition = record.partition()
                    # offset = record.offset()
                    # timestamp = record.timestamp()
                    # 取出msgKey與msgValue
                    msgKey = try_decode_utf8(record.key())
                    msgValue = try_decode_utf8(record.value())

                    sendmsg_items = {
                        msgKey: msgValue
                    }
                    client = pymongo.MongoClient(
                        "mongodb+srv://peter:0987602620@cluster0.0qqo9.mongodb.net/ceb101?retryWrites=true&w=majority")

                    mydb = client.wow
                    mycol = mydb['items']
                    mycol.insert_many([sendmsg_items])

                    # 秀出metadata與msgKey & msgValue訊息
                    # print('%s-%d-%d : (%s , %s)' % (topic, partition, offset, msgKey, msgValue))
                    # test_msg = ("%s" , "%s" %(msgKey,msgValue))
                    print(sendmsg_items)
                    return sendmsg_items

            # 同步地執行commit (Sync commit)
            if records_pulled:
                offsets = consumer.commit(asynchronous=False)
                print_sync_commit_result(offsets)


    except KeyboardInterrupt as e:
        sys.stderr.write('Aborted by user\n')
    except Exception as e:
        sys.stderr.write(str(e))

    finally:

        # consumer.commit(asynchronous=False)
        # 步驟6.關掉Consumer實例的連線
        print("已收到商品訊息")
        consumer.close()
def get_items2():
    props = {
        'bootstrap.servers': '10.1.1.133:9092',  # 連接的Kafka集群
        'group.id': 'peter',  # ConsumerGroup的名稱
        'auto.offset.reset': 'latest',  # 是否從這個ConsumerGroup尚未讀取的partition/offset開始讀earliest
        'enable.auto.commit': True,  # 是否啟動自動commit
        'error_cb': error_cb  # 設定接收error訊息的callback函數
    }

    # 步驟2. 產生一個Kafka的Consumer的實例
    consumer = Consumer(props)
    # 步驟3. 指定想要訂閱訊息的topic名稱
    topicName = 'items2'
    # 步驟4. 讓Consumer向Kafka集群訂閱指定的topic
    consumer.subscribe([topicName])

    # 步驟5. 持續的拉取Kafka有進來的訊息
    try:
        while True:
            records_pulled = False  # 用來檢查是否有有效的record被取出來

            # 請求Kafka把新的訊息吐出來
            records = consumer.consume(num_messages=500, timeout=1.0)  # 批次讀取
            if records is None:
                continue

            for record in records:
                # 檢查是否有錯誤
                if record is None:
                    continue
                if record.error():
                    # Error or event
                    if record.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition event
                        sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                         (record.topic(), record.partition(), record.offset()))
                    else:
                        # Error
                        raise KafkaException(record.error())
                else:
                    records_pulled = True

                    # ** 在這裡進行商業邏輯與訊息處理 **
                    # 取出相關的metadata
                    # topic = record.topic()
                    # partition = record.partition()
                    # offset = record.offset()
                    # timestamp = record.timestamp()
                    # 取出msgKey與msgValue
                    msgKey = try_decode_utf8(record.key())
                    msgValue = try_decode_utf8(record.value())

                    sendmsg_items2 = {
                        msgKey: msgValue
                    }
                    client = pymongo.MongoClient(
                        "mongodb+srv://peter:0987602620@cluster0.0qqo9.mongodb.net/ceb101?retryWrites=true&w=majority")

                    mydb = client.wow
                    mycol = mydb['items']
                    mycol.insert_many([sendmsg_items2])

                    # 秀出metadata與msgKey & msgValue訊息
                    # print('%s-%d-%d : (%s , %s)' % (topic, partition, offset, msgKey, msgValue))
                    # test_msg = ("%s" , "%s" %(msgKey,msgValue))
                    print(sendmsg_items2)
                    return sendmsg_items2

            # 同步地執行commit (Sync commit)
            # if records_pulled:
            #     offsets = consumer.commit(asynchronous=False)
            #     print_sync_commit_result(offsets)


    except KeyboardInterrupt as e:
        sys.stderr.write('Aborted by user\n')
    except Exception as e:
        sys.stderr.write(str(e))

    finally:
        # consumer.commit(asynchronous=False)
        # 步驟6.關掉Consumer實例的連線
        print("已收到商品訊息")
def get_trans():
    props = {
        'bootstrap.servers': '10.1.1.133:9092',  # Kafka集群在那裡? (置換成要連接的Kafka集群)
        'group.id': 'peter',  # ConsumerGroup的名稱 (置換成你/妳的學員ID)
        'auto.offset.reset': 'latest',  # 是否從這個ConsumerGroup尚未讀取的partition/offset開始讀earliest
        'enable.auto.commit': False,  # 是否啟動自動commit
        'error_cb': error_cb  # 設定接收error訊息的callback函數
    }

    # 步驟2. 產生一個Kafka的Consumer的實例
    consumer = Consumer(props)
    # 步驟3. 指定想要訂閱訊息的topic名稱
    topicName = 'transaction'
    # 步驟4. 讓Consumer向Kafka集群訂閱指定的topic
    consumer.subscribe([topicName])

    # 步驟5. 持續的拉取Kafka有進來的訊息
    try:
        while True:
            records_pulled = False  # 用來檢查是否有有效的record被取出來

            # 請求Kafka把新的訊息吐出來
            records = consumer.consume(num_messages=500, timeout=1.0)  # 批次讀取
            if records is None:
                continue

            for record in records:
                # 檢查是否有錯誤
                if record is None:
                    continue
                if record.error():
                    # Error or event
                    if record.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition event
                        sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                         (record.topic(), record.partition(), record.offset()))
                    else:
                        # Error
                        raise KafkaException(record.error())
                else:
                    records_pulled = True

                    # ** 在這裡進行商業邏輯與訊息處理 **
                    # 取出相關的metadata
                    # topic = record.topic()
                    # partition = record.partition()
                    # offset = record.offset()
                    # timestamp = record.timestamp()
                    # 取出msgKey與msgValue
                    try:
                        msgKey2 = try_decode_utf8(record.key())
                        msgValue2 = try_decode_utf8(record.value())

                        sendmsg_trans = {
                            msgKey2: msgValue2
                        }
                        print(sendmsg_trans)
                        return sendmsg_trans

                    finally:
                        client = pymongo.MongoClient(
                            "mongodb+srv://peter:0987602620@cluster0.0qqo9.mongodb.net/ceb101?retryWrites=true&w=majority")

                        mydb = client.wow
                        mycol = mydb['fit']
                        mycol.insert_many([sendmsg_trans])

                    # 秀出metadata與msgKey & msgValue訊息
                    # print('%s-%d-%d : (%s , %s)' % (topic, partition, offset, msgKey, msgValue))
                    # test1_msg = ("%s" , "%s" % (msgKey, msgValue)) #('%s : %s' % (msgKey,msgValue))




            # 同步地執行commit (Sync commit)
            # if records_pulled:
            #     offsets = consumer.commit(asynchronous=False)
            #     print_sync_commit_result(offsets)


    except KeyboardInterrupt as e:
        sys.stderr.write('Aborted by user\n')
    except Exception as e:
        sys.stderr.write(str(e))

    finally:
        consumer.commit(asynchronous=False)
        # 步驟6.關掉Consumer實例的連線
        consumer.close()

from datetime import *
import threading
if __name__ == '__main__':
    import time
    # a = threading.Thread(target=memberin)
    # b = threading.Thread(target=get_items)
    # c = threading.Thread(target=get_trans)
    # x = get_items()
    # y = get_trans()
    # a.start()
    # time.sleep(3)
    # b.start()
    # c.start()
    while True:
        memberin2()
        # memberin()
        try:
            a = get_items2()
            c = get_trans()
            print("辨識成功")
            b = get_items()
            print("開始合併交易訊息......")
            final_msg = {}
            time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            final_msg.setdefault("Time", time)
            final_msg.update(c)
            final_msg.update(a)
            final_msg.update(b)
            x = int(final_msg["商品1"][-4:-1])
            p = int(final_msg["商品2"][-4:-1])
            c = (x+p)
            cc = str(c)+'元'
            final_msg.setdefault("總金額", cc)
            print(final_msg)
        except BufferError as e:
            print(e)
        client = pymongo.MongoClient("mongodb+srv://peter:0987602620@cluster0.0qqo9.mongodb.net/ceb101?retryWrites=true&w=majority")

        mydb = client.wow
        mycol = mydb['transaction']
        mycol.insert_many([final_msg])
        print("已完成並上傳成功!!")






