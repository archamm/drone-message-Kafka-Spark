import React, { useState } from "react";
import { makeStyles } from "@material-ui/core/styles";
import List from "@material-ui/core/List";
import WarningIcon from "@material-ui/icons/Warning";
import ListItem from "@material-ui/core/ListItem";
import ListItemIcon from "@material-ui/core/ListItemIcon";
import ListItemText from "@material-ui/core/ListItemText";
import ListSubheader from "@material-ui/core/Divider";
import ErrorMessageDialog from "./ErrorMessageDialog";
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

  return (
    <div>
      <List
        style={{
          marginRight: "auto",
          marginLeft: "auto",
          marginTop: "25px",
          padding: "30px",
          borderRadius: "30px",
        }}
        component="nav"
        aria-labelledby="nested-list-subheader"
        subheader={
          <ListSubheader
            style={{ marginBottom: "30px" }}
            component="div"
            id="nested-list-subheader"
          >
            Drone Code 100 Message List
          </ListSubheader>
        }
        className={classes.root}
      >
        {props.messages.map((m, i) => (
          <ErrorMessageItem
            message={m}
            key={i}
            handleClickOpen={handleClickOpen}
          />
        ))}
      </List>
      <ErrorMessageDialog open={open} setOpen={setOpen} message={currentMsg}/>
    </div>
  );
};
export default ErrorMessageList;
