<?php
include("../database.php");
  $agencia= $_POST['nombre'];
  $anunciante= $_POST['celular'];
  $campana= $_POST['username'];
  $medios= $_POST['password'];

  $query="INSERT INTO `administrador`(`nombre`, `celular`, `username`, `password`) VALUES ('$nombre','$celular','$username','$password')";
  $resultado= $conn->query($query);
  if($resultado){
    header("Location: /views/administrador.php");
  }
  else{
    echo "Insercion no exitosa";
  }

?>
