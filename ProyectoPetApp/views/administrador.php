<?php include ('../php/session_start_Admin.php'); ?>
<!DOCTYPE html>
<html lang="es">

  <head>
    <meta charset="utf-8">
    <title>PETYA APP - ADMIN</title>
    
    <!-- Load font awesome icons y fonts -->
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/4.7.0/css/font-awesome.min.css">
    <link href="https://fonts.googleapis.com/css?family=Roboto" rel="stylesheet">
    <script src="https://ajax.googleapis.com/ajax/libs/jquery/3.3.1/jquery.min.js"></script>

    <!-- Load CSS externo -->
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0-alpha1/dist/css/bootstrap.min.css" rel="stylesheet" integrity="sha384-GLhlTQ8iRABdZLl6O3oVMWSktQOp6b7In1Zl3/Jr59b6EGGoI1aFkw7cmDA6j6gD" crossorigin="anonymous">
    <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0-alpha1/dist/js/bootstrap.bundle.min.js" integrity="sha384-w76AqPfDkMBDXo30jS1Sgez6pr3x5MlQ1ZAGC+nuZB+EYdgRZgiwxhTBTkF7CXvN" crossorigin="anonymous"></script>
    
    <!-- Datatables -->
    <link rel="stylesheet" href="https://cdn.datatables.net/v/bs4-4.1.1/dt-1.10.18/datatables.min.css">
    <script src="https://cdn.datatables.net/v/bs4-4.1.1/dt-1.10.18/datatables.min.js"></script>
        <style>
            ul {
              list-style-type: none;
              margin: 0;
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

            .active {
              background-color: #04AA6D;
            }

            #mydatatable tfoot input{
                width: 100% !important;
            }
            #mydatatable tfoot {
                display: table-header-group !important;
            }
        </style>

  </head>

<body>
    <?php if(!empty($admin)): ?>
        <nav>
            <ul>
              <li><a class="active" href="./administrador.php">INICIO</a></li>
              <li><a href="./form_correo.php">ENVAR CORREO</a></li>
              <li><a href="./mi_perfil_Admin.php">MI PERFIL</a></li>
              <li style="float:right"><a href="../loginadmin.php">CERRAR SECION</a></li>
            </ul>
        </nav>

        <div class="table-responsive container-fluid" id="mydatatable-container">
            <br><h1><b>LISTA DE USUARIOS REGISTRADOS</b></h1>
        <table class="records_list table table-striped table-bordered table-hover" id="mydatatable">
        <thead>
              <tr>
                  <th><b>ACCION</b></th>
                  <th><b>NOMBRE DEL DUEÑO</b></th>
                  <th><b>NOMBRE DE LA MASCOTA</b></th>
                  <th><b>CELULAR</b></th>
                  <th><b>CORREO</b></th>
                  <th><b>FECHA DE CUMPLEAÑOS</b></th>
              </tr>
          </thead>
            <tfoot>
                <tr>
                    <th>Filtrar..</th>
                    <th>Filtrar..</th>
                    <th>Filtrar..</th>
                    <th>Filtrar..</th>
                    <th>Filtrar..</th>
                    <th>Filtrar..</th>
                </tr>
            </tfoot>
            <tbody>
                <?php
                    include("../php/database.php");
                    $query="SELECT * FROM lista ORDER BY id DESC";
                    $resultado= $conn->query($query);
                    while($row=$resultado->fetch(PDO::FETCH_ASSOC)){
                ?>

                <tr>
                    <td>
                        <a href="../php/eliminarUsuario.php?id=<?php echo $row['id']; ?>"><botton class="btn btn-danger"><i class="fa fa-trash" aria-hidden="true"></i> ELIMINAR</booton></a>
                    </td>

                    <td><?php echo $row['nombre_d']; ?></td>
                    <td><?php echo $row['nombre_m']; ?></td>
                    <td><?php echo $row['celular']; ?></td>
                    <td><?php echo $row['correo']; ?></td>
                    <td><?php echo $row['cumpleaneos']; ?></td>

                </tr>
                <?php
                    }
                 ?>
        </table>
    </div>
        <script type="text/javascript">
        $(document).ready(function() {
            $('#mydatatable tfoot th').each( function () {
                var title = $(this).text();
                $(this).html( '<input type="text" placeholder="Filtrar.." />' );
            } );
        
            var table = $('#mydatatable').DataTable({
                "dom": 'B<"float-left"i><"float-right"f>t<"float-left"l><"float-right"p><"clearfix">',
                "responsive": false,
                "language": {
                    "url": "https://cdn.datatables.net/plug-ins/1.10.19/i18n/Spanish.json"
                },
                "order": [[ 0, "desc" ]],
                "initComplete": function () {
                    this.api().columns().every( function () {
                        var that = this;
        
                        $( 'input', this.footer() ).on( 'keyup change', function () {
                            if ( that.search() !== this.value ) {
                                that
                                    .search( this.value )
                                    .draw();
                                }
                        });
                    })
                }
            });
        });
        </script>

    <?php else: ?>
        <h1>Por favor Inicia Secion antes de Continuar</h1>
    <?php endif; ?>
</body>
</html>

