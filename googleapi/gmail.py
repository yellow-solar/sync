""" Set of functions to interact with gmail API
        - alerts
        - reports etc.
 """

#  standard
import os, json, base64
from email import encoders
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication

# third party
from httplib2 import Http
# from oauth2client import file, client, tools
from google.oauth2 import service_account
from googleapiclient import discovery, errors

# Google drive API config
class Gmail():
    """gmail service creator with input account file"""
    def __init__(self, filename, user):
        self.filename = filename
        self.user = user
        self.scopes = ['https://mail.google.com/',
                        # 'https://www.googleapis.com/auth/gmail.compose',
                        # 'https://www.googleapis.com/auth/gmail.send',
                        ]
        self.credentials = (service_account
            .Credentials.from_service_account_file(
                self.filename, 
                scopes=self.scopes,
                subject=self.user,
                )
        )    
        # Initiate gdrive service
        self.service = discovery.build('gmail', 'v1',
            credentials=self.credentials)

    def create_message(self, sender, to, subject, message_text, html=None, files={}):
        """Create a message for an email.

        Args:
            sender: Email address of the sender.
            to: Email address of the receiver.
            subject: The subject of the email message.
            message_text: The text of the email message.

        Returns:
            An object containing a base64url encoded email object.
        """
        message = MIMEMultipart()
        message['to'] = to
        message['from'] = sender
        message['subject'] = subject

        # Attach parts of email
        # html
        if html is None:
            # plain text
            message.attach(MIMEText(message_text))
        else:
            html_part = MIMEText(html,'html')
            message.attach(html_part)
        
        # file attachments
        for filename in files.keys():
            attachment = MIMEBase('application', "octet-stream")
            # attachment.set_payload(open(f"{files[filename]}", "rb").read())
            attachment.set_payload(files[filename])
            encoders.encode_base64(attachment)
            attachment.add_header(
                'Content-Disposition',
                f"attachment; filename= {filename}",
                )
            message.attach(attachment)

        return {'raw': base64.urlsafe_b64encode(message.as_bytes()).decode()}
        # msg.set_payload(contents)
        # Encode the payload using Base64.  This line is from here:
        # https://docs.python.org/3/library/email-examples.html
        # encoders.encode_base64(msg)

    def send_message(self, message, user_id="me"):
        """Send an email message.

        Args:
            service: Authorized Gmail API service instance.
            user_id: User's email address. The special value "me"
            can be used to indicate the authenticated user.
            message: Message to be sent.

        Returns:
            Sent Message.
        """
        try:
            message = (self.service.users().messages()
                    .send(userId=user_id, body=message)
                    .execute())
            print(f"Message Id: {message['id']}")
            return message
        except errors.HttpError as error:
            print(f"An error occurred: {error}")
            raise

    def quick_send(self, to, subject, text):
        msg = self.create_message(self.user, to, subject, text)
        self.send_message(msg)

# Credentials object creation with config file
if __name__ == "__main__":
    # Create email service
    gmail = Gmail('googleservice/mail-93851bb46b8d.json', 'system@yellow.africa')
    gmail.quick_send('ben@yellow.africa, bwalwyn@gmail.com','test', 'content is here...')




