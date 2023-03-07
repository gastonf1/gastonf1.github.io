<?php
include("./database.php");
  $nombre_d = $_POST['nombre_d'];
  $nombre_m = $_POST['nombre_m'];
  $celular = $_POST['celular'];
  $correo = $_POST['correo'];
  $cumpleaneos = $_POST['cumpleaneos'];

  $query="INSERT INTO `lista`(`nombre_d`, `nombre_m`, `celular`, `correo`, `cumpleaneos`) VALUES('$nombre_d','$nombre_m','$celular','$correo','$cumpleaneos')";
  $resultado= $conn->query($query);
  if($resultado){
    header("Location: /");
  }
  else{
    echo "Insercion no exitosa";
  }

?>
