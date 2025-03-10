# data-collector-telegram

![](/imgs/telegram_diagram.svg)

To set up access to the Telegram API, one has to:
- Follow [theses instructions](https://core.telegram.org/api/obtaining_api_id) to register.
- Set the environment variables `AI4TRUST_PHONE_NUMBER`, `AI4TRUST_API_ID` and `AI4TRUST_API_HASH` using those provided at the end of registration.
- Locally, run the following script just once
    ```python
    client = collegram.client.connect(api_id, api_hash, phone_nr)
    print(collegram.client.string_session_from(client.session))
    ```
    and set the env variable `AI4TRUST_TG_SESSION` using its output.
    This one time,     you'll get prompted for a code received by message, in order to grant access to your
    account to your API client.
    Saving the session string allows you not to have to go through this prompt again in the future.
