import React, { useState } from "react";
import { makeStyles } from "@material-ui/core/styles";
import List from "@material-ui/core/List";
import WarningIcon from "@material-ui/icons/Warning";
import ListItem from "@material-ui/core/ListItem";
import ListItemIcon from "@material-ui/core/ListItemIcon";
import ListItemText from "@material-ui/core/ListItemText";
import ErrorMessageDialog from "./ErrorMessageDialog";
import ClearIcon from "@material-ui/icons/Clear";
import IconButton from '@material-ui/core/IconButton';

const useStyles = makeStyles((theme) => ({
  root: {
    width: "100%",
    maxWidth: 360,
    backgroundColor: theme.palette.background.paper,
  },
}));

const ErrorMessageItem = (props) => {
  const message = JSON.parse(props.message);
  console.log(message);
  return (
    <ListItem
      divider={true}
      key={props.key}
      button
      onClick={(e) => props.handleClickOpen(message)}
    >
      <ListItemIcon>
        <WarningIcon />
      </ListItemIcon>
      <ListItemText
        primary={"DroneID" + message.DroneId}
        secondary={message.date.substring(11)}
      />
    </ListItem>
  );
};

const ErrorMessageList = (props) => {
  const classes = useStyles();
  const [open, setOpen] = useState(false);
  const [currentMsg, setCurrentMsg] = useState(null);

  const handleClickOpen = (message) => {
    setCurrentMsg(message);
    setOpen(true);
  };
  const deleteMessage = (message) => {
    props.setMessages(props.messages.filter((m) => m !== message));
  };

  return (
    <div>
      <List
        style={{
          marginRight: "auto",
          marginLeft: "auto",
          marginTop: "25px",
          padding: "30px",
          borderRadius: "30px",
          border: "3px solid black",
        }}
        className={classes.root}
      >
        <h2 style={{ marginBottom: "30px" }}>Drone Code 100 Message List </h2>
        {props.messages.map((m, i) => (
          <div style={{display: 'flex', flexDirection: 'row'}}>
            <ErrorMessageItem
              message={m}
              key={i}
              handleClickOpen={handleClickOpen}
            />
            <IconButton
              style={{ marginLeft: "auto" }}
              onClick={(e) => deleteMessage(m)}
            >
              <ClearIcon />
            </IconButton>
          </div>
        ))}
      </List>
      <ErrorMessageDialog open={open} setOpen={setOpen} message={currentMsg} />
    </div>
  );
};
export default ErrorMessageList;
