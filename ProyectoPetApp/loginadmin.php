<?php

  session_start();

  if (isset($_SESSION['admin_id'])) {
    header('Location: ./index.php');
  }
  require 'php/database.php';

  if (!empty($_POST['username']) && !empty($_POST['password'])) {
    $records = $conn->prepare('SELECT id, username, password FROM administrador WHERE username = :username'); 
    $records->bindParam(':username', $_POST['username']);
    $records->execute();
    $results = $records->fetch(PDO::FETCH_ASSOC);

    $message = '';

    if (count($results) > 0 && password_verify($_POST['password'], $results['password'])) {
      $_SESSION['admin_id'] = $results['id'];
      header("Location: ./views/administrador.php");
    } else {
      $message = 'Lo siento, esas credenciales no coinciden';
    }
  }

?>

<!DOCTYPE html>
<html>
  <head>
    <meta charset="utf-8">
    <title>Inicia Secion - ADMINISTRADOR</title>
    <link href="https://fonts.googleapis.com/css?family=Roboto" rel="stylesheet">
    <link rel="stylesheet" href="assets/css/login.css">
  </head>
  <body>
    <?php if(!empty($message)): ?>
      <p> <?= $message ?></p>
    <?php endif; ?>

    <h1>Inicia Secion - ADMINISTRADOR</h1>
    
    <form action="loginadmin.php" method="POST">
      <input name="username" type="text" placeholder="Introduce tu Username">
      <input name="password" type="password" placeholder="Ingresa tu Contraseña">
      <input type="submit" value="Inicia Secion">
    </form>
  </body>
</html>
