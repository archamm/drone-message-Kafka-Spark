import React, { useState, useEffect } from 'react';
import getSQSMessages from './component/UpdateMessageQueue'
import ErrorMessageList from './component/ErrorMessageList'
import background from './images/imageBackground.jpeg'

function App() {
  const [messages, setMessages] = useState([])
  useEffect(() => {
    document.body.style = 'background: red;'
    document.body.style = 'fon'
    setInterval(async () => {
      getSQSMessages(messages, setMessages)
      console.log(messages)
    }, 10000);
  }, [messages])
  console.log('array from Message', Array.from(new Set(messages)))

  return (
    <div styles={{ backgroundImage:`url(${background})` }}>
      <ErrorMessageList messages={messages}/>
    </div>
  );
}

export default App;
