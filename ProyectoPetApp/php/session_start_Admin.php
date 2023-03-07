<?php
  session_start();

  require 'database.php';

  if (isset($_SESSION['admin_id'])) {
    $records = $conn->prepare('SELECT * FROM administrador WHERE id = :id');
    $records->bindParam(':id', $_SESSION['admin_id']);
    $records->execute();
    $results = $records->fetch(PDO::FETCH_ASSOC);

    $user = null;

    if (count($results) > 0) {
      $admin = $results;
    }
  }
?>