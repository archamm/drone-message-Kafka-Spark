import React from "react";
import Button from "@material-ui/core/Button";
import Dialog from "@material-ui/core/Dialog";
import DialogActions from "@material-ui/core/DialogActions";
import DialogContent from "@material-ui/core/DialogContent";
import DialogContentText from "@material-ui/core/DialogContentText";
import DialogTitle from "@material-ui/core/DialogTitle";
import CardActionArea from '@material-ui/core/CardActionArea';
import CardMedia from '@material-ui/core/CardMedia';

import { Card } from "@material-ui/core";

export default function ErrorCodeMessageDialog(props) {
  const { open, setOpen, message } = props;

  const handleClose = () => {   
    setOpen(false);
  };
  return message == null ? null :
   (
    <Dialog
      fullWidth={"lg"}
      maxWidth={"lg"}
      open={open}
      onClose={handleClose}
      aria-labelledby="max-width-dialog-title"
    >
      <DialogTitle id="max-width-dialog-title">{"Details on Code 100 message from " + message.DroneId}</DialogTitle>
      <DialogContent>
        <DialogContentText>
            {"Taken at " +  message.date.substring(11) + " on " + message.date.substring(0, 10)}
        </DialogContentText>
        <Card>
          <CardActionArea>
            <CardMedia
              component="iframe"
              src={"https://maps.google.com/maps?q=" + message.latitude + ", " + message.longitude + "&z=15&output=embed"}
              title="Location"
            />
          </CardActionArea>
        </Card>
      </DialogContent>
      <DialogActions>
        <Button onClick={handleClose} color="primary">
          Close
        </Button>
      </DialogActions>
    </Dialog>
  );
}
