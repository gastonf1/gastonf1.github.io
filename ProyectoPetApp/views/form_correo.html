<?php
  date_default_timezone_set('America/Montevideo');
  include ('../php/session_start_Admin.php'); 
?>
<!DOCTYPE html>
<html lang="es">
  <head>
    <meta charset="utf-8">
    <title>PETYA APP - ADMIN</title>
    <meta charset="utf-8">
    <link href="https://fonts.googleapis.com/css?family=Roboto" rel="stylesheet">
    <link rel="stylesheet" href="../../assets/css/form.css">
      <style>
        ul {
          list-style-type: none;
          margin: -8px;;
          padding: 0;
          overflow: hidden;
          background-color: #333;
        }

        li {
          float: left;
          border-right:1px solid #bbb;
        }

        li:last-child {
          border-right: none;
        }

        li a {
          display: block;
          color: white;
          text-align: center;
          padding: 14px 16px;
          text-decoration: none;
        }

        li a:hover:not(.active) {
          background-color: #111;
        }

        input[type=text], select, textarea {
          width: 100%;
          padding: 12px;
          border: 1px solid #ccc;
          border-radius: 4px;
          resize: vertical;
        }

        label {
          padding: 12px 12px 12px 0;
          display: inline-block;
        }

        input[type=submit] {
          background-color: #04AA6D;
          color: white;
          padding: 12px 20px;
          border: none;
          border-radius: 4px;
          cursor: pointer;
          float: right;
        }

        input[type=submit]:hover {
          background-color: #45a049;
        }

        .container {
          border-radius: 5px;
          background-color: #f2f2f2;
          padding: 20px;
        }

        .col-25 {
          float: left;
          width: 25%;
          margin-top: 6px;
        }

        .col-75 {
          float: left;
          width: 75%;
          margin-top: 6px;
        }

        /* Clear floats after the columns */
        .row:after {
          content: "";
          display: table;
          clear: both;
        }

        /* Responsive layout - when the screen is less than 600px wide, make the two columns stack on top of each other instead of next to each other */
        @media screen and (max-width: 600px) {
          .col-25, .col-75, input[type=submit] {
            width: 100%;
            margin-top: 0;
          }
        }
      </style>
</head>

<body>
  <?php if(!empty($admin)): ?>
  <nav>
      <ul>
        <li><a href="./administrador.php">INICIO</a></li>
        <li><a class="active" href="./form_correo.php">ENVAR CORREO</a></li>
        <li><a href="./mi_perfil_Admin.php">MI PERFIL</a></li>
        <li style="float:right"><a href="../loginadmin.php">CERRAR SECION</a></li>
      </ul>
  </nav>

    <?php
      include("../php/database.php");
      $query="SELECT * FROM lista";
      $resultado= $conn->query($query);
      $row=$resultado->fetch(PDO::FETCH_ASSOC);
    ?>

    <form id="main-contact-form" class="contact-form" name="contact-form" action="../php/enviarCorreo.php" method="POST">
      <div class="col-sm-5 col-sm-offset-1">
          <div class="form-group">
             <label>Nombre *</label>
             <input type="text" id="nombre" name="nombre" class="form-control" required="required">
           </div>
           <div class="form-group">
              <label>Email *</label>
              <input type="email" id="email" name="email" class="form-control" required="required" value="<?php echo $row['correo']; ?>">
           </div> 
           <div class="form-group">
               <label>Asunto *</label>
               <input type="text" id="subject" name="subject" class="form-control" required="required">
           </div>
           <div class="form-group">
                <label>Mensaje *</label>
                <textarea name="mensaje" id="mensaje" required="required" class="form-control" rows="8"></textarea>
           </div>
           <div class="form-group">
                <input type="submit" id="submit" name="submit" class="btn btn-primary btn-lg" value="Enviar" />
           </div>                      
      </div> 
    </form>

    <?php else: ?>
      <h1>Por favor Inicia Secion antes de Continuar</h1>
    <?php endif; ?>
  </body>
</html>

  
