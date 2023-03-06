<?php
include("../database.php");
    $id= $_REQUEST['id'];
    $query="DELETE FROM lista WHERE id='$id'";
    $resultado= $conn->query($query);
  if($resultado){
    header("Location: /views/administrador.php");
  }
  else{
    echo "Insercion no exitosa";
  }

?>
