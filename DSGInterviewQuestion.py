#Michael Swartzentruber
#DSG interview question
#12/2/22

import sys
import requests
import json
import snowflake.connector
import os
import smtplib
from email.message import EmailMessage

#define the send of email function
def sendEmail(newUser_emailAddress, firstName, lastName, password, userName):
    #establish senders email credentials
    sendersEmail = "XXXXXXX@gmail.com"
    sendersPassword = "xxxxxx"
    #establish the message for the email using the standard email functionallity from our import of smtplib
    msg = EmailMessage()
    msg['Subject'] = 'Snowflake username and password change'
    msg['From'] = sendersEmail
    # pass in the newUser_emailAddress we have from our API call
    msg['To'] = newUser_emailAddress
    #creating the body of the message
    messageText = "hello " + firstName + " " + lastName + " "+ "Your snowflake login has been created."
    passwordText = "password: " + password + "\n" + "userName:" + userName
    msg.set_content(messageText+'\n'+ passwordText)
    #using the smtplib library we send out the email
    with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
        smtp.login(sendersEmail, sendersPassword)
        smtp.send_message(msg)


def sendErrorEmail():
    #establish the senders email credentials
    sendersEmail = "xxxxxxxx@gmail.com"
    sendersPassword = "xxxxxxx"
    #establish the message for the email using the standard email functionallity from our import of smtplib
    msg = EmailMessage()
    msg['Subject'] = 'Snowflake username and password change'
    msg['From'] = sendersEmail
    # Send the email to our DB admins so they are aware of the fact that the user wasnt created
    msg['To'] = 'db_admins@somecompany.co'
    messageText = "User " + firstName + " " + lastName + "'s' "+ "snowflake account was not created."
    msg.set_content(messageText)
    #using the smtplib library we send out the email
    with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
        smtp.login(sendersEmail, sendersPassword)
        smtp.send_message(msg)


def createUsers():
    userInformation =[]
    #establish our tracking variable to end when we have 10 users
    track = 0
    #create 10 users with the name, username, password, and email
    while track < 10:
        #call the api with the specific fields we are looking for
        response = requests.get("https://randomuser.me/api/?inc=name,login,email&nat=us")
        # load the data with the imported json library
        rawData = json.loads(response.text)
        tmpList=[]
        # set the values of the jason equal to our tmplist
        for key in rawData:
            if key == 'results':
                # gather name first and last, username, password, and email for the new user
                tmpList.append(rawData[key][0]['name']['first'])
                tmpList.append(rawData[key][0]['name']['last'])
                tmpList.append(rawData[key][0]['login']['username'])
                tmpList.append(rawData[key][0]['login']['password'])
                tmpList.append(rawData[key][0]['email'])
                # add the new list to our 2 dimentional list so we have all the values in one place
                # preparing to pass this when complete
                userInformation.append(tmpList)
        track = track + 1
    # pass our 2d array back so we are able to see each users information in one place
    return userInformation


# connect to snowflake
ctx = snowflake.connector.connect(
    user='xxxxxxxx',
    password='xxxxxxxxxx',
    account='xxxxxxx',
    region = 'us-east-2.aws'
    )
cs = ctx.cursor()

try:
    # call our api call function
    listOfUsers = createUsers()
except:
    # call our failure function if necessary
    sendErrorEmail()
    #break out of everything so we dont unintentinally send the DB team emeails twice
    exit()

try:
    #go through each user
    for user in listOfUsers:
        # create our sql statement
        sqlStat = "create user " + user[2] + " password= '" + user[3] + "' default_role=readonlyrole;"
        # execute our sql statement
        cs.execute(sqlStat)
        #call our send email function so our user is notified
        sendEmail(user[4],user[0],user[1], user[3], user[2])
except:
    #send our db team notification of the failure
    sendErrorEmail()
    exit()
#close our snowflake connections for the cursor and the connector
cs.close()
ctx.close()
