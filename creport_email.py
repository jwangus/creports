from _secrets import email_config as conf
import smtplib
from email.message import EmailMessage


def send_html(html_msg, to_addr, from_addr, subject):
    msg = EmailMessage()
    msg['Subject'] = subject
    msg['from'] = from_addr
    msg['to'] = to_addr
    msg.set_content('Good Morning in plain text')
    msg.add_alternative(html_msg, subtype='html')

    with smtplib.SMTP_SSL(conf.smtp_server, 465) as smtp:
        smtp.login(conf.smtp_server_login, conf.smtp_server_pwd)
        smtp.send_message(msg)
