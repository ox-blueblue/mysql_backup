import os
from datetime import datetime
import paramiko
import schedule
import time
from blueutils.config_manager import ConfigManager
from blueutils.feishu_robot import FeishuRobot
from loguru import logger as log

config_manager = ConfigManager('config.ini')
config_manager.set('docker', 'host', '') 
config_manager.set('docker', 'name', '') 
config_manager.set('ssh', 'user', '')
config_manager.set('ssh', 'port', 22)
config_manager.set('ssh', 'passwd', '')
config_manager.set('mysql', 'user', '')
config_manager.set('mysql', 'passwd', '')
config_manager.set('mysql', 'dbname', '')
config_manager.set('backup', 'remotedir', '')
config_manager.set('backup', 'localdir', '')
config_manager.set('backup', 'time', '02:00')
config_manager.set('report', 'url', '')
config_manager.set('report', 'key', '')
config_manager.save() 
# 配置信息
DOCKER_HOST = config_manager.get('docker', 'host')  # Docker 主机 IP 或者域名
DOCKER_SSH_PORT = config_manager.get_int('ssh', 'port')  # 默认SSH端口，如果有更改请调整
DOCKER_SSH_USER = config_manager.get('ssh', 'user')
DOCKER_SSH_PASSWD = config_manager.get('ssh', 'passwd')  # 如果使用密钥认证，请提供私钥路径
DOCKER_CONTAINER_NAME = config_manager.get('docker', 'name')  # Docker 容器名称
MYSQL_USER = config_manager.get('mysql', 'user')
MYSQL_PASSWORD = config_manager.get('mysql', 'passwd')
MYSQL_DB_NAME = config_manager.get('mysql', 'dbname')
BACKUP_REMOTE_DIR = config_manager.get('backup', 'remotedir')  # 在设备 B 上保存备份的位置
BACKUP_LOCAL_DIR = config_manager.get('backup', 'localdir')  # 在设备 B 上保存备份的位置
BACKUP_TIME = config_manager.get('backup', 'time')
REPORT_URL = config_manager.get('report', 'url')
REPORT_KEY = config_manager.get('report', 'key')

feishu = FeishuRobot(webhook=REPORT_URL, secret=REPORT_KEY)
feishu.send_text(text=f"数据库每日备份任务开始运行，备份时间：{BACKUP_TIME}")
def backup_database():
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    backup_file_remote = BACKUP_REMOTE_DIR+f"/{MYSQL_DB_NAME}_{timestamp}.sql"
    backup_file_local = os.path.join(BACKUP_LOCAL_DIR, f"{MYSQL_DB_NAME}_{timestamp}.sql")

    try:
        feishu.send_text(text=f"数据库开始备份...")
        # 创建SSH客户端
        ssh_client = paramiko.SSHClient()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh_client.connect(DOCKER_HOST, port=DOCKER_SSH_PORT, username=DOCKER_SSH_USER, password=DOCKER_SSH_PASSWD)

        # 执行 mysqldump 命令并保存到临时文件
        cmd = f'docker exec {DOCKER_CONTAINER_NAME} ' \
               f'mysqldump -u{MYSQL_USER} -p{MYSQL_PASSWORD} {MYSQL_DB_NAME} --single-transaction > {backup_file_remote}'
        stdin, stdout, stderr = ssh_client.exec_command(cmd)

        # 检查命令是否成功执行
        if stdout.channel.recv_exit_status() == 0:
            log.success(f"Backup created successfully on remote host: {backup_file_remote}")
        else:
            log.error(f"mysqldump failed: {stderr.read().decode()}")
            feishu.send_text(text=f"数据库备份错误:{stderr.read().decode()}", is_at_all=True)
            return

        # 使用 SFTP 传输文件到本地
        sftp = ssh_client.open_sftp()
        sftp.get(backup_file_remote, backup_file_local)
        sftp.close()

        log.debug(f"Backup transferred to local: {backup_file_local}")
        feishu.send_text(text=f"数据库备份成功:{backup_file_local}")
        # 删除远程临时备份文件
        # ssh_client.exec_command(f'rm {backup_file_remote}')
        # ssh_client.close()

        #log.debug(f"Remote temporary file deleted: {backup_file_remote}")
    except Exception as e:
        log.error(f"An error occurred: {e}")

# 设置每天执行一次备份
schedule.every().day.at(BACKUP_TIME).do(backup_database) 

if __name__ == "__main__":
    while True:
        schedule.run_pending()
        time.sleep(60)  # 每分钟检查一次是否有任务要运行