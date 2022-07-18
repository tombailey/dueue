type Message = {
  id: string;
  body: string;
  expiry?: Date;
  acknowledgementDeadlines?: Record<string, Date>;
  acknowledgements?: Set<string>;
};

export default Message;
