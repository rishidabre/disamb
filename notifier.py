#!/usr/bin/python

#import socket

def send_notification(sender, receivers, subject, text):
    SERVER = ""
    #FROM = socket.gethostname()+"@syr.edu"
    FROM = sender
    TO = receivers # must be a list

    SUBJECT = subject
    TEXT = text

    # Prepare actual message
    message = """From: %s\r\nTo: %s\r\nSubject: %s\r\n\

    %s\n\n** This is a system generated message.
    """ % (FROM, ", ".join(TO), SUBJECT, TEXT)

    # Send the mail
    import smtplib
    server = smtplib.SMTP(SERVER)
    server.sendmail(FROM, TO, message)
    server.quit()
    return
