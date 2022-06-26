import Message from "./message"

type NewMessage = {
  body: Message["body"];
  expiry?: Message["expiry"];
};

export default NewMessage;
