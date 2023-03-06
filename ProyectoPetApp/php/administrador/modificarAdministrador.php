<?php
include("../database.php");
  $id= $_REQUEST['id'];
  $nombre= $_POST['nombre'];
  $celular= $_POST['celular'];
  $username= $_POST['username'];
  $password= password_hash($_POST['password'], PASSWORD_BCRYPT);

  $query="UPDATE `administrador` SET 
                                    `nombre`=' $nombre',
                                    `celular`='$celular',
                                    `username`='$username',
                                    `password`='$password'
                               WHERE id='$id'";
                                       
  $resultado= $conn->query($query);
  if($resultado){
    header("Location: /views/administrador.php");
  }
  else{
    echo "Insercion no exitosa";
  }

?>
