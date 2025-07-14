# 🔔 How to Set Up Discord Webhook Alerts for Monitoring

This guide explains how to configure a Discord webhook and test it using `curl`. You can use this to receive alerts from Airflow, scripts, or any automation pipeline.

---

## 📦 Prerequisites

- A Discord account
- Access to a Discord server (create your own if needed)

---

## ⚙️ Step-by-Step: Creating a Discord Webhook

### 1. Create or Open Your Server

If you don't have a Discord server:
- Click the ➕ icon in the left sidebar
- Choose **Create My Own**
- Name it (e.g., `fraud-alerts`)

---

### 2. Create a Channel for Alerts

- Right-click in your server panel → **Create Channel**
- Select **Text Channel**
- Name it something like `#monitoring-alerts`
- Click **Create**

---

### 3. Create a Webhook in Discord

1. Click on your **server name** → **Server Settings**
2. Go to **Integrations** → then **Webhooks**
3. Click **New Webhook**
4. Set a name (e.g., `AirflowBot`)
5. Choose the channel `#monitoring-alerts`
6. Click **Copy Webhook URL**
   - It will look like:
     ```
     https://discord.com/api/webhooks/123456789012345678/AbCdEfGhIjKlMnOpQrStUvWxYz...
     ```

---

### 4. Save the Webhook URL in Environment Variables

In your `.env` file (or system environment), store it like this:

````
DISCORD\_WEBHOOK\_URL="https://discord.com/api/webhooks/123456789012345678/AbCdEfGhIjKlMnOpQrStUvWxYz"
````

You’ll use this variable from your code or DAG to send alerts.

---

## 🧪 Test the Webhook Using curl

Run the following command in your terminal (replace with your actual URL):

```bash
curl -X POST \
  -H "Content-Type: application/json" \
  -d '{"content": "✅ *Test Discord Alert* – webhook is working perfectly!"}' \
  https://discord.com/api/webhooks/123456789012345678/AbCdEfGhIjKlMnOpQrStUvWxYz...
````

✅ If successful, a message will appear instantly in your selected Discord channel.

---

## ✅ You're All Set!

You can now send alerts from your ML monitoring pipeline, DAG, or any backend script directly to your Discord server.