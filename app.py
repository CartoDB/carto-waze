import requests
import sendgrid
import logging
from flask import Flask, request, render_template, jsonify, url_for
from werkzeug.contrib.fixers import ProxyFix
from sendgrid.helpers.mail import Email, Content, Mail

from oauth import protect_api
from oauthlib.oauth1.rfc5849.utils import unescape
from requests_oauthlib import OAuth1Session

from config import DEBUG, EMAIL_RECIPIENTS, CONFIRMATION_URL, CLIENT_KEY, CLIENT_SECRET, EMAIL_SENDER, SENDGRID_APIKEY

if DEBUG is True:
    logging.basicConfig(level=logging.DEBUG)

app = Flask(__name__)
app.wsgi_app = ProxyFix(app.wsgi_app)
sg = sendgrid.SendGridAPIClient(SENDGRID_APIKEY)
oauth_client = OAuth1Session(CLIENT_KEY, CLIENT_SECRET, None, None)


def send_email(mail):
    if (SENDGRID_APIKEY != "test_apikey"):
        sg.client.mail.send.post(request_body=mail.get())


@app.route("/api/analytics/v1/create")
@protect_api(app)
def create():
    logging.info("Create event")

    try:
        event_url = unescape(request.args.get("eventUrl"))
    except TypeError:
        logging.error("Couldn't get event URL (%s)", request.args)
        return jsonify({"success": "false", "url": request.url}), requests.codes.bad_request
    else:
        logging.info("Event URL (%s)", event_url)

    event_response = oauth_client.get(event_url, headers={"accept": "application/json"})
    logging.info("Event response %s (%s)", event_response, event_response.status_code)

    try:
        event = event_response.json()
    except ValueError:
        logging.error("Event response didn't have json content (%s)", event_response.text)
        return jsonify({"success": "false", "event_response": event_response.status_code}), requests.codes.bad_request
    else:
        logging.info("Event (%s)", event)
        event_id = event_url.strip("/").rsplit('/', 1)[-1]

    try:
        event_type = event["type"]
    except KeyError:
        logging.error("Event object didn't have a type (%s)", event)
        return jsonify({"success": "false", "event": event}), requests.codes.bad_request
    else:
        logging.info("Event type %s", event_type)
        if event_type != "SUBSCRIPTION_ORDER":
            logging.error("Event is not a subscription order (%s)", event_type)
            return jsonify({"success": "false", "event_type": event_type}), requests.codes.bad_request

    provisioned_url = "{base_url}?subscription_id={event_id}&carto_user=CARTO_ACCOUNT".format(base_url=url_for("confirm", _external=True), event_id=event_id)
    logging.info("Provisioned URL %s", provisioned_url)
    try:
        email_body = Content("text/plain", render_template("create_email.txt", provisioned_url=provisioned_url, **event))
    except KeyError:
        logging.error("Email couldn't be prepared")
        return jsonify({"success": "false", "provisioned_url": provisioned_url}), requests.codes.bad_request

    from_email = Email(EMAIL_SENDER)
    for recipient in EMAIL_RECIPIENTS:
        logging.info("Email sent %s", recipient)
        mail = Mail(from_email, "New Vodafone Analytics subscription", Email(recipient), email_body)
        send_email(mail)

    logging.info("Create success")
    return jsonify({"success": "true"}), requests.codes.accepted


@app.route("/api/analytics/v1/confirm")
def confirm():
    logging.info("Confirm event")

    event_id = request.args.get("subscription_id")
    carto_user = request.args.get("carto_user")
    logging.info("Event id %s", event_id)
    logging.info("CARTO user %s", carto_user)
    if carto_user == "CARTO_ACCOUNT":
        logging.error("Wrong CARTO user")
        return render_template("error.html", carto_user=carto_user, event_id=event_id)

    notification_response = oauth_client.post(CONFIRMATION_URL.format(event_id=event_id), json={"accountIdentifier": carto_user, "success": True})

    if notification_response.ok is True:
        logging.info("Confirm success")
        return render_template("confirm.html", carto_user=carto_user)
    else:
        logging.warning("Notification response failed %s (%s)", notification_response, notification_response.status_code)
        return render_template("error.html", carto_user=carto_user, event_id=event_id)


@app.route("/api/analytics/v1/change")
@protect_api(app)
def change():
    logging.info("Change event")

    try:
        event_url = unescape(request.args.get("eventUrl"))
    except TypeError:
        logging.error("Couldn't get event URL (%s)", request.args)
        return jsonify({"success": "false", "url": request.url}), requests.codes.bad_request
    else:
        logging.info("Event URL %s", event_url)

    event_response = oauth_client.get(event_url, headers={"accept": "application/json"})
    logging.info("Event_response %s (%s)", event_response, event_response.status_code)

    try:
        event = event_response.json()
    except ValueError:
        logging.error("Event response didn't have json content (%s)", event_response.text)
        return jsonify({"success": "false", "event_response": event_response.status_code}), requests.codes.bad_request
    else:
        event_id = event_url.strip("/").rsplit('/', 1)[-1]
        logging.info("Event (%s)", event)

    try:
        event_type = event["type"]
    except KeyError:
        logging.error("Event object didn't have a type (%s)", event)
        return jsonify({"success": "false", "event": event}), requests.codes.bad_request
    else:
        logging.info("Event type %s", event_type)
        if event_type != "SUBSCRIPTION_CHANGE":
            logging.error("Event is not a subscription change (%s)", event_type)
            return jsonify({"success": "false", "event_type": event_type}), requests.codes.bad_request

    provisioned_url = "{base_url}?subscription_id={event_id}&carto_user=CARTO_ACCOUNT".format(base_url=url_for("confirm", _external=True), event_id=event_id)
    logging.info("Provisioned URL %s", provisioned_url)
    try:
        email_body = Content("text/plain", render_template("change_email.txt", provisioned_url=provisioned_url, **event))
    except KeyError:
        logging.error("Email couldn't be prepared")
        return jsonify({"success": "false", "provisioned_url": provisioned_url}), requests.codes.bad_request

    from_email = Email(EMAIL_SENDER)
    for recipient in EMAIL_RECIPIENTS:
        logging.info("Email sent %s", recipient)
        mail = Mail(from_email, "Existing Vodafone Analytics subscription has beed updated", Email(recipient), email_body)
        send_email(mail)

    logging.info("Change success")
    return jsonify({"success": "true"}), requests.codes.accepted


@app.route("/api/analytics/v1/cancel")
@protect_api(app)
def cancel():
    logging.info("Cancel event")

    try:
        event_url = unescape(request.args.get("eventUrl"))
    except TypeError:
        logging.error("Couldn't get event URL (%s)", request.args)
        return jsonify({"success": "false", "url": request.url}), requests.codes.bad_request
    else:
        logging.info("Event URL %s", event_url)

    event_response = oauth_client.get(event_url, headers={"accept": "application/json"})
    logging.info("Event response %s (%s)", event_response, event_response.status_code)

    try:
        event = event_response.json()
    except ValueError:
        logging.error("Event response didn't have json content (%s)", event_response.text)
        return jsonify({"success": "false", "event_response": event_response.status_code}), requests.codes.bad_request
    else:
        event_id = event_url.strip("/").rsplit('/', 1)[-1]
        logging.info("Event (%s)", event)

    try:
        event_type = event["type"]
    except KeyError:
        logging.error("Event object didn't have a type (%s)", event)
        return jsonify({"success": "false", "event": event}), requests.codes.bad_request
    else:
        logging.info("Event type %s", event_type)
        if event_type != "SUBSCRIPTION_CANCEL":
            logging.error("Event is not a subscription cancel (%s)", event_type)
            return jsonify({"success": "false", "event_type": event_type}), requests.codes.bad_request

    provisioned_url = "{base_url}?subscription_id={event_id}&carto_user=CARTO_ACCOUNT".format(base_url=url_for("confirm", _external=True), event_id=event_id)
    logging.info("Provisioned URL %s", provisioned_url)
    try:
        email_body = Content("text/plain", render_template("cancel_email.txt", provisioned_url=provisioned_url, **event))
    except KeyError:
        logging.error("Email couldn't be prepared")
        return jsonify({"success": "false", "provisioned_url": provisioned_url}), requests.codes.bad_request

    from_email = Email(EMAIL_SENDER)
    for recipient in EMAIL_RECIPIENTS:
        logging.info("Email sent %s", recipient)
        mail = Mail(from_email, "Existing Vodafone Analytics subscription has beed canceled", Email(recipient), email_body)
        send_email(mail)

    logging.info("Cancel success")
    return jsonify({"success": "true"}), requests.codes.accepted
