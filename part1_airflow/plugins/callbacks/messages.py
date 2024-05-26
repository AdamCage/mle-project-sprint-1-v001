from airflow.providers.telegram.hooks.telegram import TelegramHook


TELEGRAM_CONN_ID = 'test'
TOKEN = '7004553650:AAGALahCTPusEh_HTeEI27IvGCLXT8QOKFA'
CHAT_ID = '958987446'


def send_telegram_message(message):
    hook = TelegramHook(telegram_conn_id=TELEGRAM_CONN_ID, token=TOKEN, chat_id=CHAT_ID)
    hook.send_message({
        'chat_id': CHAT_ID,
        'text': message
    })


def send_telegram_success_message(context):
    dag_id = context['dag'].dag_id
    run_id = context.get('run_id', 'unknown_run_id')
    
    message = f'Success: The DAG "{dag_id}" completed successfully with run_id "{run_id}".'
    send_telegram_message(message)


def send_telegram_failure_message(context):
    dag_id = context['dag'].dag_id
    run_id = context.get('run_id', 'unknown_run_id')
    task_instance_key_str = context.get(
        'task_instance_key_str', 'unknown_task_instance')
    
    message = f'Failure: The DAG "{dag_id}" failed to complete with run_id "{run_id}". Task instance key: "{task_instance_key_str}".'
    send_telegram_message(message)
