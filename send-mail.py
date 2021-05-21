import aiosqlite
import asyncio
import aiosmtplib
from email.message import EmailMessage


self_email = "admin@admin.local"
addr_mail_server = "127.0.0.1"
port_mail_server = "1025"

async def send_mail_async(sender, to, first_name, last_name, subject):
    message = EmailMessage()
    message["From"] = sender
    message["To"] = to
    message["Subject"] = subject
    text = 'Уважаемый {} {} !\nСпасибо, что пользуетесь нашим сервисом'.format(first_name, last_name)
    message.set_content(text)
    try:
        await aiosmtplib.send(message, hostname=addr_mail_server, port=port_mail_server)
        print('Message sended to {}'.format(to))
    except:
        await asyncio.sleep(2)
        print('Not connect to mail server')


async def get_email():
    async with aiosqlite.connect('contacts.db') as db:
        db.row_factory = aiosqlite.Row
        async with db.execute('SELECT * FROM contacts') as cursor:
            async for row in cursor:
                email_to = row['email']
                first_name = row['first_name']
                last_name = row['last_name']
                await send_mail_async(self_email, email_to, first_name, last_name, "Объявление")


def main():
    loop = asyncio.get_event_loop()
    loop.run_until_complete(get_email())
    loop.close()

if __name__ == '__main__':
    main()
