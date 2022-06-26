type Message = {
  id: string;
  body: string;
  expiry?: Date;
  restore?: Date;
};

export default Message;
