import AWS from 'aws-sdk'

const getSQSMessages = (messages, setMessages) => {

    AWS.config.update({region: 'eu-west-3', accessKeyId: 'AKIAS7AOU2S4KEKI3Q7K', secretAccessKey: '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', }) 
    const sqs = new AWS.SQS({apiVersion: '2012-11-05'});
    
    const queueURL =  'https://sqs.eu-west-3.amazonaws.com/204041671864/drones-code-100-messages.fifo	'

    const params = {
        AttributeNames: [
           "SentTimestamp"
        ],
        MaxNumberOfMessages: 10,
        MessageAttributeNames: [
           "All"
        ],
        QueueUrl: queueURL,
        VisibilityTimeout: 20,
        WaitTimeSeconds: 0
       };
       sqs.receiveMessage(params, function(err, data) {
         if (err) {
           console.log("Receive Error", err);
         } else if (data.Messages && data.Messages[0]) {
             console.log(data.Messages)
             setMessages(Array.from(new Set(messages.concat(data.Messages.map(m => m.Body)))))
           /*var deleteParams = {
             QueueUrl: queueURL,
             ReceiptHandle: data.Messages[0].ReceiptHandle
           };
           sqs.deleteMessage(deleteParams, function(err, data) {
             if (err) {
               console.log("Delete Error", err);
             } else {
               console.log("Message Deleted", data);
             }
           });*/
         }
       });};

export default getSQSMessages