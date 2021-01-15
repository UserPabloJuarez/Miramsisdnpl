#!/usr/bin/perl

use DBI;
use ExtUtils::MakeMaker qw(prompt WriteMakefile);


require("/home/uprog/bin/mvnoFunciones.pl" or die "ERROR MUY GRAVE: No encuentro mvnoFunciones.pl");
require("/home/uprog/bin/mvnoFuncionesServicios.pl" or die "ERROR MUY GRAVE: No encuentro mvnoFuncionesServicios.pl");
require("/home/uprog/bin/mvnoFuncionesProductos.pl" or die "ERROR MUY GRAVE: No encuentro mvnoFuncionesProductos.pl");


$db_mvno = DBI->connect("DBI:mysql:mvno:srv_mvno",$user_name, ,) or &log_error("E-TVT2-001", "Error en la base de datos mvno");
$db_mcdr = DBI->connect("DBI:mysql:mcdr:srv_mcdr",$user_name, ,) or &log_error("E-TVT2-002", "Error en la base de datos mcdr");
$db_mcdi = DBI->connect("DBI:mysql:mcdi:srv_mcdi",$user_name, ,) or &log_error("E-TVT2-002", "Error en la base de datos mcdr");
$db_mcdc = DBI->connect("DBI:mysql:mcdc:srv_mcdc",$user_name, ,) or &log_error("E-TVT2-002", "Error en la base de datos mcdr");
$db_mcdf = DBI->connect("DBI:mysql:mcdf:srv_mcdf",$user_name, ,) or &log_error("E-TVT2-003", "Error en la base de datos mcdf");
$db_mlog = DBI->connect("DBI:mysql:mlog:srv_mlog",$user_name, ,) or &log_error("E-TVT2-004", "Error en la base de datos mlog");
$db_xprv = DBI->connect("DBI:mysql:xprv:srv_xprv",$user_name, ,) or &log_error("E-TVT2-005", "Error en la base de datos xprv");
$db_xrpt = DBI->connect("DBI:mysql:xrpt:srv_xrpt",$user_name, ,) or &log_error("E-TVT2-006", "Error en la base de datos xrpt");
$db_mnet = DBI->connect("DBI:mysql:mnet:srv_mnet",$user_name, ,) or &log_error("E-TVT2-006", "Error en la base de datos mnet");
$db_xtts = DBI->connect("DBI:mysql:xtts:srv_xtts",$user_name, ,) or &log_error("E-TVT2-006", "Error en la base de datos xtts");
$db_mopb = DBI->connect("DBI:mysql:mopb:srv_mopb",$user_name, ,) or &log_error("E-TVT2-006", "Error en la base de datos mopb");

 $fecha=`/bin/date +"%Y%m%d"`;
 chomp($fecha);
 $hora=`/bin/date +"%H%M%S"`;
 chomp($hora);

 $fecha_mes_siguiente=`/bin/date --date "1 month" +"%Y%m"`;
 chomp($fecha_mes_siguiente);
 $fecha_mes_siguiente=$fecha_mes_siguiente."01";


$msisdn=$ARGV[0].$ARGV[1].$ARGV[2].$ARGV[3];


if ($msisdn eq "")
 {
  $msisdn=prompt("Dime el Msisdn o el iccid:",$msisdn);
 }
 
  #if ($T_MigradoCRM ne 0) {
	#print "Atención! La linea introducida se encuentra migrada y no es posible modificarla. Por favor, indique otra linea"."\n";
	#$msisdn=prompt("Dime el Msisdn o el iccid:","");
	#}

  $msisdn=~ s/\ //g; 
  $msisdn=~ s/\.//g; 
  $msisdn=~ s/\t//g; 

$opcion="";


$contador_clientes=0;

$qry_clientes=qq{SELECT Id_Cliente, Msisdn, Id_Estado_Actual FROM pv_msisdn WHERE Id_Cliente='$msisdn'};
$st_clientes=$db_mvno->prepare($qry_clientes);
$st_clientes->execute();

while ( ($qId_Cliente, $qMsisdn, $qIdEstadoActual)=$st_clientes->fetchrow_array() )
 {
  $contador_clientes++;
  print "Msisdn: $qMsisdn ($qIdEstadoActual)   Cliente: $qId_Cliente\n";  
  $msisdn=$qMsisdn;
 }

$st_clientes->finish();

if ($contador_clientes==1)
  {
   # $msisdn=$qMsisdn;
  }

if ($contador_clientes>1)
  {
   print "\nHay mas de un msisdn asignado al cliente $msisdn. Por favor indica el msisdn.\n";
   $opcion="fin";
  }


print "Msisdn: $msisdn\n";


while ($opcion ne "fin")

{



$qry_msisdn=qq{SELECT Id_Mvno, Msisdn, Id_Submvno, Imsi, Iccid, Id_Producto, Fecha_Activacion, Hora_Activacion, Id_Estado_Actual,
                      Fecha_Estado_Actual, Hora_Estado_Actual,
                      Id_Portabilidad, Id_Cliente, Id_Cliente_Fact, Id_Empresa, Id_Tarifa, Id_Prepago, Id_Prepago_Mvno,
                      Riesgo_Limite, Riesgo_Balance, Riesgo_Balance_Real, Riesgo_Margen, Riesgo_Variacion, Numeracion_Reciclada,
                      Id_Tarifa, Id_Idioma, Id_Moneda, MigradoCRM
                 FROM pv_msisdn
               WHERE Msisdn='$msisdn' OR Iccid='$msisdn' OR Id_Cliente='$msisdn'};

$st_msisdn=$db_mvno->prepare($qry_msisdn);
$st_msisdn->execute();
($T_Id_Mvno, $T_Msisdn, $T_Id_Submvno, $T_Imsi, $T_Iccid, $T_Id_Producto, $T_Fecha_Activacion, $T_Hora_Activacion, $T_Id_Estado_Actual,
 $T_Fecha_Estado_Actual, $T_Hora_Estado_Actual,
 $T_Id_Portabilidad, $T_Id_Cliente, $T_Id_Cliente_Fact, $T_Id_Empresa, $T_Id_Tarifa, $T_Id_Prepago, $T_Id_Prepago_Mvno,
 $N_Riesgo_Limite, $N_Riesgo_Balance, $N_Riesgo_Balance_Real, $N_Riesgo_Margen, $N_Riesgo_Variacion, $N_Numeracion_Reciclada,
 $T_Id_Tarifa, $T_Id_Idioma, $T_Id_Moneda, $T_MigradoCRM)=$st_msisdn->fetchrow_array();
$st_msisdn->finish();


if ($T_Iccid eq "") { $T_Iccid=$msisdn;}

$qry_iccid=qq{SELECT Msisdn_Asociado, Imsi, Id_Producto, Id_Estado, Envio_Almacen
                     FROM pv_iccid
               WHERE Iccid='$msisdn' OR Iccid='$T_Iccid'};
$st_iccid=$db_mvno->prepare($qry_iccid);
$st_iccid->execute();
($ic_Msisdn_Asociado, $ic_Imsi, $ic_Id_Producto, $ic_Id_Estado, $ic_Envio_Almacen)=$st_iccid->fetchrow_array();
$st_iccid->finish();

if ($T_Msisdn ne "")
  {
   $msisdn=$T_Msisdn;
  }

$qry_cliente=qq{SELECT Id_Cliente, Id_Cliente_Fact, Nombre, Razon_Social, Nif, Id_Estado_Cliente, Id_Grupo_Cliente
                  FROM pv_clientes
                 WHERE Id_Cliente='$T_Id_Cliente'};
$st_cliente=$db_xprv->prepare($qry_cliente);
$st_cliente->execute();
($er_Id_Cliente, $er_Id_Cliente_Fact, $T_Nombre, $T_Razon_Social, $T_Nif, $T_Id_Estado_Cliente, $T_Id_Grupo_Cliente)=$st_cliente->fetchrow_array();
$st_cliente->finish();

$qry_grupo_cliente=qq{SELECT Id_Submvno FROM pv_clientes_grupos WHERE Id_Grupo_Cliente='$T_Id_Grupo_Cliente'};
$st_grupo_cliente=$db_xprv->prepare($qry_grupo_cliente);
$st_grupo_cliente->execute();
($T_Submvno_Cliente)=$st_grupo_cliente->fetchrow_array();
$st_grupo_cliente->finish();


$qry_cliente_almacen=qq{SELECT Id_Cliente, Id_Cliente_Fact, Nombre
                  FROM pv_clientes
                 WHERE Id_Cliente='$ic_Envio_Almacen'};
$st_cliente_almacen=$db_xprv->prepare($qry_cliente_almacen);
$st_cliente_almacen->execute();
($almacen_Id_Cliente, $almacen_Id_Cliente_Fact, $almacen_Nombre)=$st_cliente_almacen->fetchrow_array();
$st_cliente_almacen->finish();

 $qry_mvno=qq{SELECT Descripcion FROM pv_mvno WHERE id_mvno='$T_Id_Mvno'};
 $st_mvno=$db_mvno->prepare($qry_mvno);
 $st_mvno->execute();
 ($descripcion_mvno)=$st_mvno->fetchrow_array();
 $st_mvno->finish();

 
 if ($T_Msisdn eq "") {
 print "ATENCION!!! El msisdn $T_Msisdn no existe en nuestras tablas, usaremos para las consultas el $msisdn.\n";
 $T_Msisdn=$msisdn;
 $T_Iccid ="";
 }
 
 
 if ($T_MigradoCRM ne 0) {
	print "Atencion! La linea introducida se encuentra migrada y no es posible modificarla."."\n";
	exit();	
 }
 elsif($T_MigradoCRM ne 1)
{
	print "La linea existe como no migrado: $T_MigradoCRM es correcto!"."\n";
}else
{
	print " ";
}


print "-------------------------------------------------------------------------------------------\n";
print "				  MVNO: $descripcion_mvno ($T_Id_Mvno)\n";
print "		  		Msisdn: $T_Msisdn - $T_Id_Submvno - $T_Id_Producto - Prepago: $T_Id_Prepago ($T_Id_Prepago_Mvno)\n";
print "   Fecha Activacion: $T_Fecha_Activacion, ($T_Hora_Activacion)\n";
print "             Estado: $T_Id_Estado_Actual - Fecha: $T_Fecha_Estado_Actual, ($T_Hora_Estado_Actual)\n";
print "             Tarifa: $T_Id_Tarifa - Moneda: $T_Id_Moneda - Idioma: $T_Id_Idioma\n";
print "              Iccid: $T_Iccid\n";
print "               Imsi: $ic_Imsi\n";
print "   Msisdn Migracion: $T_MigradoCRM\n";
print "-------------------------------------------------------------------------------------------\n";
print " Cliente (pv_msisdn)  : $T_Id_Cliente  Id_Cliente_Fact: $T_Id_Cliente_Fact\n";
print " Cliente (pv_clientes): $er_Id_Cliente  Id_Cliente_Fact: $er_Id_Cliente_Fact Grupo: $T_Id_Grupo_Cliente($T_Submvno_Cliente)\n";
print "       Estado cliente : $T_Id_Estado_Cliente\n";
print "                Nombre: $T_Nombre\n";
print "          Razon Social: $T_Razon_Social\n";
print "                   Nif: $T_Nif\n";
print " 	Datos Sim Almacen:\n";
print "         Estado Actual: $ic_Id_Estado\n";
print "      Producto Almacen: $ic_Id_Producto       Envio Almacen: $ic_Envio_Almacen ($almacen_Nombre)\n";
print "       Msisdn Asociado: $ic_Msisdn_Asociado\n";
print "-------------------------------------------------------------------------------------------\n";


$qry_bonos=qq{SELECT Id_Bono AS B, (SELECT Id_Submvno FROM pv_mvno_bonos WHERE Id_Bono=B AND Fecha_Inicio<='$fecha' AND Fecha_Fin>='$fecha' ) as Id_Submvno, (SELECT Descripcion FROM pv_mvno_bonos WHERE Id_Bono=B AND Fecha_Inicio<='$fecha' AND Fecha_Fin>='$fecha' ) as Descripcion, (SELECT Fecha_Activacion FROM pv_mvno_bonos WHERE Id_Bono=B AND Fecha_Inicio<='$fecha' AND Fecha_Fin>='$fecha') AS Bono_Activacion, Id_Cliente,Flag_Activado FROM pv_mvno_bonos_msisdn Where Id_Cliente='$T_Id_Cliente' AND Msisdn='$T_Msisdn'  AND Flag_Activado='S'};
$st_bonos=$db_mvno->prepare($qry_bonos);
$st_bonos->execute();
while ( ($er_Bono, $er_Id_Submvno, $er_Descripcion,$er_Bono_Activacion, $er_Id_Cliente_Bono,$er_Flag_Activado_Bono)=$st_bonos->fetchrow_array() )
  {
   print "  Bonos: $er_Bono, $er_Id_Submvno, $er_Bono_Activacion, $er_Descripcion, $er_Id_Cliente_Bono, $er_Flag_Activado_Bono\n";
  }
$st_bonos->finish();



$qry_bonos=qq{SELECT Id_Bono AS B, (SELECT Id_Submvno FROM pv_mvno_bonos WHERE Id_Bono=B AND Fecha_Inicio<='$fecha' AND Fecha_Fin>='$fecha' ) as Id_Submvno, (SELECT Descripcion FROM pv_mvno_bonos WHERE Id_Bono=B AND Fecha_Inicio<='$fecha' AND Fecha_Fin>='$fecha' ) as Descripcion, (SELECT Fecha_Activacion FROM pv_mvno_bonos WHERE Id_Bono=B AND Fecha_Inicio<='$fecha' AND Fecha_Fin>='$fecha') AS Bono_Activacion, Id_Cliente,Flag_Estado FROM pv_mvno_bonos_adicionales Where Id_Cliente='$T_Id_Cliente' AND Msisdn='$T_Msisdn'};
$st_bonos=$db_mvno->prepare($qry_bonos);
$st_bonos->execute();
while ( ($er_Bono, $er_Id_Submvno, $er_Descripcion,$er_Bono_Activacion, $er_Id_Cliente_Bono,$er_Flag_Activado_Bono)=$st_bonos->fetchrow_array() )
  {
   print "  Bonos Adicionales: $er_Bono, $er_Id_Submvno, $er_Bono_Activacion, $er_Descripcion, $er_Id_Cliente_Bono, $er_Flag_Activado_Bono\n";
  }
$st_bonos->finish();


$qry_promos=qq{SELECT Id_Promo AS P, 
                      (SELECT Id_Submvno FROM pv_mvno_promociones WHERE Id_Promo=P) as Id_Submvno, 
                      (SELECT Descripcion FROM pv_mvno_promociones WHERE Id_Promo=P) as Descripcion, 
                      Id_Cliente 
                 FROM pv_mvno_promociones_msisdn Where Id_Cliente='$T_Id_Cliente' AND Msisdn='$T_Msisdn' AND Flag_Activado='S'};
$st_promos=$db_mvno->prepare($qry_promos);
$st_promos->execute();
while (($er_Promo, $er_Id_Submvno, $er_Descripcion, $er_Promo_Activacion, $er_Cliente_Promo, $er_Flag_Activado)=$st_promos->fetchrow_array() )
  {
   print "  Promos: $er_Promo, $er_Id_Submvno, $er_Descripcion, $er_Promo_Activacion, $er_Cliente_Promo, $er_Flag_Activado\n";
  }
$st_promos->finish();

print "\n";
if ($T_Submvno_Cliente ne $T_Id_Submvno)
  {
    print "  ----> ATENCION!: El cliente tiene un grupo de cliente que no corresponde al submvno $T_Id_Submvno. Esto ocasiona problemas de facturacion. Hay que corregirlo!!!\n";
   }

if ($T_Id_Mvno ne "A") {
 print "  ----> ATENCION!!! El msisdn es $descripcion_mvno ($T_Id_Mvno). Tener mucho cuidado con las operaciones, ya que algunas no podrian estar finalizadas.\n";
 }


print "\n-------------------------------------------------------------------------------------------\n";
print " 01. Detalle en tablas\n";
print " 02. Cdrs y resumen llamadas\n";
print " 03. Saldo, Recargas, Cargos, Ajuste de Margen Prepago\n";
print " 04. Servicios Red (datos,roaming,tarificacion avanzada premium)\n";
print " 05. Identificacion y activacion\n";
print " 06. Portabilidades\n";
print " 07. Productos (promociones, bonos)\n";
if ($T_Id_Prepago_Mvno eq "P")
  {
   print " 10. Ciclo Vida Prepago\n";
  }
if ($T_Id_Prepago_Mvno eq "T")
  {
   print " 11. Ciclo Vida Postpago\n";
  }
print " 12. Migracion Perfil Electrico/Postpago y cambio de SIM\n";
print " 13. Facilidades de Red\n";
print " 14. Cambios de Titular y Retarificaciones\n";
print " 15. Bajas de Lineas, y Clientes\n";
print " 90. Logs del sistema\n";

print "\n";

 $opcion= prompt("Dime el tipo de cambio que quieres realizar (fin para salir)");

 print "\n";
 if ($opcion eq "01") { &detalle($T_Msisdn, $T_Id_Cliente); }
 if ($opcion eq "02") { &cdrs($T_Msisdn, $T_Id_Cliente); }
 if ($opcion eq "03") { &saldo_recargas_cargos($T_Msisdn, $T_Id_Cliente); }
 if ($opcion eq "04") { &servicios($T_Msisdn, $T_Id_Cliente); }
 if ($opcion eq "05") { &activaciones($T_Msisdn, $T_Id_Cliente); }
 if ($opcion eq "06") { &portabilidad($T_Msisdn, $T_Id_Cliente); }
 if ($opcion eq "07") { &productos($T_Msisdn, $T_Id_Cliente); }
 if ($opcion eq "10") { &ciclo_vida_prepago($T_Msisdn, $T_Id_Cliente); }
 if ($opcion eq "11") { &ciclo_vida_postpago($T_Msisdn, $T_Id_Cliente); }
 if ($opcion eq "12") { &migracion_perfil_electrico($T_Msisdn, $T_Id_Cliente); }
 if ($opcion eq "13") { &facilidades_red($T_Msisdn, $T_Id_Cliente); }
 if ($opcion eq "14") { &retarificaciones($T_Msisdn, $T_Id_Submvno, $T_Id_Cliente, $T_Id_Cliente_Fact); }
 if ($opcion eq "15") { &bajas($T_Msisdn, $T_Id_Cliente, $T_Id_Cliente_Fact, $T_Id_Submvno); }
 if ($opcion eq "90") { &logs($T_Msisdn, $T_Id_Cliente); }
 print "------------------------------------\n\n";
}









$db_mvno->disconnect();
$db_mcdr->disconnect();
$db_mcdi->disconnect();
$db_mcdc->disconnect();
$db_mcdf->disconnect();
$db_mlog->disconnect();
$db_xprv->disconnect();
$db_xrpt->disconnect();
$db_mnet->disconnect();
$db_xtts->disconnect();
$db_mopb->disconnect();



exit(0);

#01 Detalle en tablas

sub detalle

{

 my($T_Msisdn, $T_Id_Cliente)=@_;


 $opcion2="";

while ($opcion2 ne "99")

 {
 print "-------------------------------\n";
 print "| 1. Detalle de tablas        |\n";
 print "-------------------------------\n";
 print "  Msisdn: $T_Msisdn Prepago: $T_Id_Prepago ($T_Id_Prepago_Mvno)\n";
 print " 1.1 Consulta pv_msisdn\n";
 print " 1.2 Consulta pv_iccid (todos con el msisdn asociado de $T_Msisdn)\n";
 print " 1.3 Consulta log_msisdn\n";
 print " 1.4 Consulta log_mvno_riesgo_postpago\n";
 print " 1.5 Consulta pv_clientes\n";
 print " 1.6 Consulta pv_clientes_notas\n";
 print " 1.7 Consulta sms enviados\n";

 print "  99 Menu Principal\n";
 
 $opcion2= prompt("Dime que quieres hacer");
 
 if ($opcion2 eq "1.1") { &consulta("mvno","SELECT * FROM pv_msisdn WHERE Msisdn='$T_Msisdn'","pv_msisdn","L"); }
 if ($opcion2 eq "1.2") { &consulta("mvno","SELECT * FROM pv_iccid WHERE Msisdn_Asociado='$T_Msisdn'","pv_iccid","L"); }
 if ($opcion2 eq "1.3") { &consulta("mlog","SELECT * FROM log_msisdn WHERE Msisdn='$T_Msisdn' ORDER BY Fecha, Hora","log_msisdn","G"); }
 if ($opcion2 eq "1.4") { &consulta("mlog","SELECT * FROM log_mvno_riesgo_postpago WHERE Msisdn='$T_Msisdn' ORDER BY Fecha, Hora","log_mvno_riesgo_postpago","G"); }
 if ($opcion2 eq "1.5") { $cliente=prompt ("Dime codigo cliente:",$T_Id_Cliente);&consulta("xprv","SELECT * FROM pv_clientes WHERE Id_Cliente='$cliente'","pv_clientes","L"); }
 if ($opcion2 eq "1.6") { $cliente=prompt ("Dime codigo cliente:",$T_Id_Cliente);&consulta("xprv","SELECT * FROM pv_clientes_notas WHERE Id_Cliente='$cliente'","pv_clientes_notas","L"); }
 if ($opcion2 eq "1.7") 
    { $cliente=prompt ("Dime codigo cliente:",$T_Id_Cliente);
      &consulta("mnet","SELECT Fecha_Solicitud,Hora_Solicitud,Tipo_Informe,Mensaje,Enviado FROM pv_mvno_envio_sms WHERE Id_Cliente='$T_Id_Cliente' AND Sms_Numero='$T_Msisdn'","pv_mvno_envio_sms","L"); }
 }
 return(0);
} # portabilidad






# 02 Cdrs

sub cdrs

{
 my($T_Msisdn, $T_Id_Cliente)=@_;


 $opcion2="";

  $fecha_desde=`/bin/date --date="1 month ago" +"%Y%m%d"`;
 chomp($fecha_desde);

  $fecha_hasta=`/bin/date --date="1 day ago" +"%Y%m%d"`;
 chomp($fecha_hasta);


while ($opcion2 ne "99")

 {
 print "------------------------------\n";
 print "| 2. Cdrs y resumen llamadas |\n";
 print "------------------------------\n";
 print "  Msisdn: $T_Msisdn Cliente: $T_Id_Cliente\n";

 print " 2.1 Consulta Detallada Cdrs mfac por dia\n";
 print " 2.2 Consulta Resumen Cdrs entre fechas\n";
 print " 2.3 Consulta Resumen Cdrs entre fechas por msisdn\n";
 print " 2.4 Consulta Resumen acumulado entre fechas\n";
 print " 2.5 Consulta Resumen acumulado entre fechas por Tipo Cdr\n";
 print " 2.6 Resumen de Llamadas entrantes entre fechas\n";
 print " 2.7 Resumen Cdrs Conciliacion Orange mcdc entre fechas\n";
 print "  99 Menu Principal\n";

 $opcion2= prompt("Dime que quieres hacer");

 if ($opcion2 eq "2.1") { 
                          $fecha=prompt("Dime la fecha",$fecha);
                          &consulta("mcdf","SELECT * FROM mvnof_cdrs$fecha  WHERE Calling_Number='$T_Msisdn' ORDER BY HORA","cdrs_fecha","L"); 
                        }

 if ($opcion2 eq "2.2") { 
                          $fecha_desde=prompt("Desde fecha",$fecha_desde);
                          $fecha_hasta=prompt("Desde fecha",$fecha_hasta);

                          @datos=`/home/uprog/bin/miraCdrsMsisdn $T_Msisdn $fecha_desde $fecha_hasta`;            

                          foreach $datos(@datos) { print $datos; }       

                   
#                          $desde_tabla="mvnof_cdrs".$fecha_desde;
#                          $hasta_tabla="mvnof_cdrs".$fecha_hasta;
#
#                          $st_tablas = $db_mcdf->prepare ("SHOW TABLES");
#                          $st_tablas -> execute();
# 
#                          while (($que_tabla)=$st_tablas->fetchrow_array())
#                               {
#                                 if ( ($que_tabla ge $desde_tabla) && ($que_tabla le $hasta_tabla) )
#                                   {
#                                    if (length($que_tabla)>16)
#                                      {
#                                        &consulta("mcdf","SELECT Fecha,Hora,Tipo_Cdr, MVNO_Tipo_Llamada, Chart_Time_Sec, Id_Destino, Tarifa_Precio, Precio FROM $que_tabla  WHERE Calling_Number='$T_Msisdn' ORDER BY HORA","cdrs_fecha","G"); 
#                                      }
#                                   }
#                               }
#                          $st_tablas -> finish();
                        }

 if ($opcion2 eq "2.3") {
                          $fecha_desde=prompt("Desde fecha",$fecha_desde);
                          $fecha_hasta=prompt("Desde fecha",$fecha_hasta);

                                 

                          $desde_tabla="mvnof_cdrs".$fecha_desde;
                          $hasta_tabla="mvnof_cdrs".$fecha_hasta;

                          $st_tablas = $db_mcdf->prepare ("SHOW TABLES");
                          $st_tablas -> execute();

                          while (($que_tabla)=$st_tablas->fetchrow_array())
                               {
                                 if ( ($que_tabla ge $desde_tabla) && ($que_tabla le $hasta_tabla) )
                                   {
                                    if (length($que_tabla)>16)
                                      {
                                        &consulta("mcdf","SELECT SUM(Chart_Time_Sec), SUM(Tarifa_Precio), SUM(Precio) FROM $que_tabla  WHERE Calling_Number='$T_Msisdn' ORDER BY HORA","cdrs_fecha","G");
                                      }
                                   }
                               }
                          $st_tablas -> finish();




                        }


 if ($opcion2 eq "2.4") { 
                          $fecha_desde=prompt("Desde fecha",$fecha_desde);
                          $fecha_hasta=prompt("Desde fecha",$fecha_hasta);
                          &consulta("mlog","SELECT * FROM inf_mvno_msisdn_dia WHERE Msisdn='$T_Msisdn' AND Fecha>='$fecha_desde' AND Fecha<='$fecha_hasta' ","cdrs","G");           
                        }

 if ($opcion2 eq "2.5") { 
                          $fecha_desde=prompt("Desde fecha",$fecha_desde);
                          $fecha_hasta=prompt("Desde fecha",$fecha_hasta);
                          &consulta("mlog","SELECT Fecha, SUM(Seconds), SUM(Data_Kb), SUM(Revenue) FROM inf_mvno_msisdn_dia WHERE Msisdn='$T_Msisdn' AND Fecha>='$fecha_desde' AND Fecha<='$fecha_hasta' GROUP BY Fecha ORDER BY Fecha","cdrs","G");           
                        }


 if ($opcion2 eq "2.6") {
                          $fecha_desde=prompt("Desde fecha",$fecha_desde);
                          $fecha_hasta=prompt("Desde fecha",$fecha_hasta);

                          @datos=`/home/uprog/bin/miraCdrsEntrantesMsisdn $fecha_desde $fecha_hasta $T_Msisdn`;

                          foreach $datos(@datos) { print $datos; }
                        }




 if ($opcion2 eq "2.7") {
                          $fecha_desde=prompt("Desde fecha",$fecha_desde);
                          $fecha_hasta=prompt("Desde fecha",$fecha_hasta);

                          $desde_tabla="mvnoc_cdrs".$fecha_desde;
                          $hasta_tabla="mvnoc_cdrs".$fecha_hasta;

                          $st_tablas = $db_mcdc->prepare ("SHOW TABLES");
                          $st_tablas -> execute();

                          while (($que_tabla)=$st_tablas->fetchrow_array())
                               {
                                 if ( ($que_tabla ge $desde_tabla) && ($que_tabla le $hasta_tabla) )
                                   {
                                    if (length($que_tabla)>16)
                                      {
                                        &consulta("mcdc","SELECT Fecha,Hora,Tipo_Cdr, Mvno_Tipo_Llamada, Called_Number, Calling_Number, TotalVolume, unitsCharged, Price, Thor_Chart_Time_Sec, Thor_Data_link FROM $que_tabla  WHERE Calling_Number LIKE '%$T_Msisdn%' OR Calling_Number='34$T_Msisdn' ORDER BY HORA","cdrs_fecha","G");
                                      }
                                   }
                               }
                          $st_tablas -> finish();
                        }


 }
 return(0);
}




sub saldo_recargas_cargos

{
 my($T_Msisdn, $T_Id_Cliente)=@_;

 print $saldo=$N_Riesgo_Limite-$N_Riesgo_Balance;


 $fecha_mes=`/bin/date --date="1 day ago" +"%Y%m"`;
 chomp($fecha_mes);


 $opcion2="";

while ($opcion2 ne "99")

 {
 print "-------------------------------\n";
 print "| 3. Saldo, Recargas y Cargos |\n";
 print "-------------------------------\n";
 print "  				Msisdn: $T_Msisdn Prepago: $T_Id_Prepago ($T_Id_Prepago_Mvno)\n";
 print "         Riesgo Limite: $N_Riesgo_Limite\n";
 print "        Riesgo Balance: $N_Riesgo_Balance\n";
 print "                 Saldo: $saldo\n";
 print "   Riesgo Balance Real: $N_Riesgo_Balance_Real\n";
 print "         Riesgo Margen: $N_Riesgo_Margen\n";
 print "      Riesgo Variacion: $N_Riesgo_Variacion\n\n";

 print " 3.1 Consulta Recargas Resumen\n";
 print " 3.2 Consulta Recargas Detalle\n";
 print " 3.3 Consulta Cargos Resumen\n";
 print " 3.4 Consulta Cargos Detalle\n";
 print " 3.5 Historico de Saldo por mes\n";
 print " 3.6 Ajuste de Margen Prepago\n";
 print "  99 Menu Principal\n";

 $opcion2= prompt("Dime que quieres hacer");

 if ($opcion2 eq "3.1") { &consulta("mlog","SELECT Fecha,Hora,Id_Tipo_Recarga, Referencia_Origen, Msisdn_Beneficiario, Importe, Iva, Efectivo FROM log_recargas  WHERE Msisdn_Beneficiario='$T_Msisdn' AND Id_Cliente_Beneficiario='$T_Id_Cliente'","log_recargas","G"); }
 if ($opcion2 eq "3.2") { &consulta("mlog","SELECT * FROM log_recargas  WHERE Msisdn_Beneficiario='$T_Msisdn' AND Id_Cliente_Beneficiario='$T_Id_Cliente'","log_recargas","L"); }
 if ($opcion2 eq "3.3") { &consulta("mlog","SELECT Fecha,Hora,Id_Tipo_Cargo, Referencia_Origen, Msisdn_Cargo, Importe, Iva, Efectivo FROM log_cargos  WHERE Msisdn_Cargo='$T_Msisdn' AND id_Cliente_Cargo='$T_Id_Cliente'","log_cargos","G"); }
 if ($opcion2 eq "3.4") { &consulta("mlog","SELECT * FROM log_cargos  WHERE Msisdn_Cargo='$T_Msisdn' AND id_Cliente_Cargo='$T_Id_Cliente'","log_cargos","L"); }
 if ($opcion2 eq "3.5") {
                          $fecha_mes=prompt("Dime la fecha (YYYYMM)",$fecha_mes);
                          &consulta("mlog","SELECT *, Riesgo_Limite-Riesgo_Balance AS Saldo FROM his_".$fecha_mes."_msisdn  WHERE Msisdn='$T_Msisdn' ORDER BY Fecha","his_msisdn","G");
                        }


 if ($opcion2 eq "3.6") {
                          &consulta("mvno","SELECT Riesgo_Margen, Riesgo_Variacion FROM pv_msisdn WHERE Msisdn='$T_Msisdn'","pv_msisdn","G");
                          $nuevo_margen= prompt("Nuevo Margen Prepago:");
                          $que_motivo=prompt("Motivo para realizar el cambio de margen:");
                          $qry_mete=qq{UPDATE pv_msisdn SET Riesgo_Margen=$nuevo_margen WHERE Msisdn='$T_Msisdn'};
                          $st_mete=$db_mvno->prepare($qry_mete);
                          $st_mete->execute();
                          $st_mete->finish();

                          print $qry_mete."\n";

                          $qry_log=qq{INSERT INTO log_msisdn SET
                                        Msisdn         ='$T_Msisdn',
                                        Id_Cliente     ='$T_Id_Cliente',
                                        Id_Cliente_Fact='$T_Id_Cliente_Fact',
                                        Tipo_Usuario   ='X',
                                        Id_Usuario     ='Thor',
                                        Fecha          ='$fecha',
                                        Hora           ='$hora',
                                        Id_Evento      ='Prepago',
                                        Id_Subevento   ='Cambio_Margen', 
                                        Descripcion    ='$que_motivo'};
                          $st_log=$db_mlog->prepare($qry_log);
                          $st_log->execute();
                          $st_log->finish();
                        } # if ($opcion2 eq "3.6")        



 }
 return(0);
}


sub servicios

{

 my($T_Msisdn, $T_Id_Cliente)=@_;


 $opcion2="";

while ($opcion2 ne "99")

 {
 print "+---------------------------------------------------------+\n";
 print "| 4. Servicios Datos, Roaming, Tarificacion Avanzada (TA) |\n";
 print "+---------------------------------------------------------+\n";
 print "  Msisdn: $T_Msisdn Prepago: $T_Id_Prepago ($T_Id_Prepago_Mvno)\n";
 print "         Riesgo Limite: $N_Riesgo_Limite\n";
 print "        Riesgo Balance: $N_Riesgo_Balance\n";
 print "                 Saldo: $saldo\n";
 print "   Riesgo Balance Real: $N_Riesgo_Balance_Real\n";
 print "         Riesgo Margen: $N_Riesgo_Margen\n";
 print "      Riesgo Variacion: $N_Riesgo_Variacion\n\n";
 print "  4.1 Consulta Estado Actual de Datos, Roaming y Tarificacion Avanzada\n";
 print "  4.2 Consulta Solicitudes de datos y roaming\n";
 print "  4.3 Consulta Transacciones Solicitudes de datos, roaming y TA\n\n";
 print " Datos.\n";
 print "  4.10 Consulta log_msisdn de Datos\n"; 
 print "  4.11 Activa Datos en Muy Baja Velocidad (GPRS-16 Kbps)\n"; 
 print "  4.12 Activa Datos en Baja Velocidad (GPRS-64 Kbps)\n"; 
 print "  4.13 Activa Datos en Alta Velocidad (HSDPA)\n";
 print "  4.14 Desactiva Datos\n\n";
 print " Roaming\n";
 print "  4.20 Consulta log_msisdn de Roaming\n";
 print "  4.21 Activa Roaming a nivel de red\n";
 print "  4.22 Desactiva Roaming a nivel de red\n\n";
 print " Tarificacion Avanzada Premium (TA)\n";
 print "  4.30 Consulta log_msisdn de TA_Premium\n";
 print "  4.31 Activa Servicios TA Llamadas Premium de Alto coste\n";
 print "  4.32 Activa Servicios TA Llamadas Premium de Bajo coste\n";
 print "  4.33 Desactiva TA Llamadas Premium\n\n";
 print "  99 Menu Principal\n";

 $opcion2= prompt("Dime que quieres hacer");

 if ($opcion2 eq "4.1") { &consulta("mvno","SELECT * FROM pv_servicios_red_msisdn WHERE Msisdn='$T_Msisdn'","pv_servicios_red_msisdn","L"); }
 if ($opcion2 eq "4.2") { 
		if ($T_Id_Mvno ne "A") {
			&consulta("mopb","SELECT * FROM  pv_opb_solicitudes_servicios_red_msisdn WHERE Msisdn='$T_Msisdn' ","pv_opb_solicitudes_servicios_red_msisdn","L");
		}else{
			&consulta("mnet","SELECT * FROM pv_solicitudes_servicios_red_msisdn WHERE Msisdn='$T_Msisdn' ","pv_solicitudes_servicios_red_msisdn","L"); 
		}
	}
 if ($opcion2 eq "4.3") { &consulta("mnet","SELECT * FROM pv_transacciones_solicitudes_servicios_red_msisdn  WHERE Msisdn='$T_Msisdn'","pv_transacciones_solicitudes_servicios_red_msisdn","L"); }
 if ($opcion2 eq "4.10") { &consulta("mlog","SELECT * FROM log_msisdn  WHERE Msisdn='$T_Msisdn' AND Id_Evento='Datos'","log_msisdn","G"); }

 if ($opcion2 eq "4.11") { 
                         $que_motivo=prompt("Motivo para la activacion de datos:");

                         $velocidad_actual=&msisdnServicioDatos($T_Msisdn,"","","",
                                              "T","miramsisdn","L",
                                              $que_motivo);
                         } # if ($opcion2 eq "4.11") 

 if ($opcion2 eq "4.12") {
                          $que_motivo=prompt("Motivo para la activacion de datos:");
                         
                          $velocidad_actual=&msisdnServicioDatos($T_Msisdn,"","","",
                                              "T","miramsisdn","M",
                                              $que_motivo);
                         } # if ($opcion2 eq "4.12") 



 if ($opcion2 eq "4.13") { 
                         $que_motivo=prompt("Motivo para la activacion de datos:");
                         $velocidad_actual=&msisdnServicioDatos($T_Msisdn,"","","",
                                              "T","miramsisdn","H",
                                              $que_motivo);
                        } # if ($opcion2 eq "4.13")                      

 if ($opcion2 eq "4.14") {

                         $que_motivo=prompt("Motivo para la activacion de datos:");
                         
                         $velocidad_actual=&msisdnServicioDatos($T_Msisdn,"","","",
                                              "T","miramsisdn","D",
                                              $que_motivo);
                        } # if ($opcion2 eq "4.14")  

   if ($opcion2 eq "4.20") { &consulta("mlog","SELECT * FROM log_msisdn  WHERE Msisdn='$T_Msisdn' AND Id_Evento='Roaming'","log_msisdn","G"); }

   if ($opcion2 eq "4.21") {
                          $que_motivo=prompt("Motivo para la activacion de datos en el Roaming:");
                         $qry_mete=qq{INSERT INTO pv_solicitudes_servicios_red_msisdn SET
                                        Msisdn         ='$T_Msisdn',
                                        Id_Estado      ='P',
                                        Roaming         = 'S',      
                                        Id_Usuario     ='miramsisdn', 
                                        Id_Cliente     ='$T_Id_Cliente',
                                        Id_Cliente_Fact='$T_Id_Cliente_Fact',
                                        Observaciones  ='Activacion de Roaming manualmente por $que_motivo'};
                          $st_mete=$db_mnet->prepare($qry_mete);
                          $st_mete->execute();
                          $st_mete->finish();
                          print "$qry_mete\n";

                           $qry_log=qq{INSERT INTO log_msisdn SET
                                        Msisdn    ='$T_Msisdn',
                                        Id_Cliente='$T_Id_Cliente',
                                        Id_Cliente_Fact='$T_Id_Cliente_Fact',
                                        Tipo_Usuario ='X',
                                        Id_Usuario='Thor',
                                        Fecha='$fecha',
                                        Hora='$hora',
                                        Id_Evento='Roaming',
                                        Id_Subevento='Activacion', 
                                        Descripcion='Activacion de Roaming manualmente por $que_motivo'};
                          $st_log=$db_mlog->prepare($qry_log);
                          $st_log->execute();
                          $st_log->finish();
                        } # if ($opcion2 eq "4.21")     
      if ($opcion2 eq "4.22") {
                          $que_motivo=prompt("Motivo para la desactivacion de datos en el Roaming:");
                         $qry_mete=qq{INSERT INTO pv_solicitudes_servicios_red_msisdn SET
                                        Msisdn         ='$T_Msisdn',
                                        Id_Estado      ='P',
                                        Roaming         = 'N',      
                                        Id_Usuario     ='miramsisdn', 
                                        Id_Cliente     ='$T_Id_Cliente',
                                        Id_Cliente_Fact='$T_Id_Cliente_Fact',
                                        Observaciones  ='Desactivacion de Roaming manualmente por $que_motivo'};
                          $st_mete=$db_mnet->prepare($qry_mete);
                          $st_mete->execute();
                          $st_mete->finish();
                          print "$qry_mete\n";

                           $qry_log=qq{INSERT INTO log_msisdn SET
                                        Msisdn    ='$T_Msisdn',
                                        Id_Cliente='$T_Id_Cliente',
                                        Id_Cliente_Fact='$T_Id_Cliente_Fact',
                                        Tipo_Usuario ='X',
                                        Id_Usuario='Thor',
                                        Fecha='$fecha',
                                        Hora='$hora',
                                        Id_Evento='Roaming',
                                        Id_Subevento='Desactivacion', 
                                        Descripcion='Desactivacion de Roaming manualmente por $que_motivo'};
                          $st_log=$db_mlog->prepare($qry_log);
                          $st_log->execute();
                          $st_log->finish();
                        } # if ($opcion2 eq "4.22")  

   if ($opcion2 eq "4.30") { &consulta("mlog","SELECT * FROM log_msisdn  WHERE Msisdn='$T_Msisdn' AND Id_Evento='TA_Premium'","log_msisdn","G"); }

   if ($opcion2 eq "4.31") {
                            print "IMPORTANTE: Activar TA Alto coste puede suponer una elevada facturacion en cliente!!!\n";
                            $que_motivo=prompt("Motivo de activacion TA premium Alto coste. Si viene por TT es aconsejable meter el numero (c cancelar):");
                            if ($que_motivo eq "c" || $que_motivo eq "C" || $que_motivo eq "")
                              {
                               print "Cancelado o es necesario indicar el motivo!!!\n\n";
                              }
                              else
                              {
                               &msisdnServicioTaPremium($T_Msisdn,"",$T_Id_Cliente, $T_Id_Cliente_Fact,"T","admin","H",$que_motivo);
                              }
                           } # if ($opcion2 eq "4.31"

   if ($opcion2 eq "4.32") {
                            $que_motivo=prompt("Motivo de activacion TA premium Bajo coste. Si viene por TT es aconsejable meter el numero (c cancelar):");
                            if ($que_motivo eq "c" || $que_motivo eq "C" || $que_motivo eq "")
                              {
                               print "Cancelado o es necesario indicar el motivo!!!\n\n";
                              }
                              else
                              {
                               &msisdnServicioTaPremium($T_Msisdn,"",$T_Id_Cliente, $T_Id_Cliente_Fact,"T","admin","L",$que_motivo);
                              }
                           } # if ($opcion2 eq "4.32"

   if ($opcion2 eq "4.33") {
                            $que_motivo=prompt("Motivo para la desactivacion TA premium si viene por TT es aconsejable meter el numero (c cancelar):");
                            if ($que_motivo eq "c" || $que_motivo eq "C" || $que_motivo eq "")
                              {
                               print "Cancelado o es necesario indicar el motivo!!!\n\n";
                              }
                              else
                              { 
                               &msisdnServicioTaPremium($T_Msisdn,"",$T_Id_Cliente, $T_Id_Cliente_Fact,"T","admin","D",$que_motivo);
                              }
                           } # if ($opcion2 eq "4.32"


   

 } 
 return(0);
}






sub activaciones

{
 my($T_Msisdn, $T_Id_Cliente)=@_;

 print $saldo=$N_Riesgo_Limite-$N_Riesgo_Balance;


 $fecha_mes=`/bin/date --date="1 day ago" +"%Y%m"`;
 chomp($fecha_mes);


 $opcion2="";

while ($opcion2 ne "99")

 {
 print "------------------------------------\n";
 print "| 5. Activaciones e identificacion |\n";
 print "-----------------------------------\n";
 print "    Msisdn: $T_Msisdn Prepago: $T_Id_Prepago ($T_Id_Prepago_Mvno)\n";
 print "         		Riesgo Limite: $N_Riesgo_Limite\n";
 print "        	   Riesgo Balance: $N_Riesgo_Balance\n";
 print "                 		Saldo: $saldo\n";
 print "   		  Riesgo Balance Real: $N_Riesgo_Balance_Real\n";
 print "         		Riesgo Margen: $N_Riesgo_Margen\n";
 print "      		 Riesgo Variacion: $N_Riesgo_Variacion\n\n";

 print " 5.1 Resumen por msisdn\n";
 print "  99 Menu Principal\n";

 $opcion2= prompt("Dime que quieres hacer");

 if ($opcion2 eq "5.1") 
   { 
    # Primero vemos las solicitudes por web y canal
     &consulta("mnet","SELECT Fecha, Hora, Id_Perfil_Activacion FROM pv_pasarela_tph_pie WHERE phoneNumber='$T_Msisdn'","pv_pasarela_tph_pie","L"); 
     &consulta("mnet","SELECT Fecha, Hora, Id_Perfil_Activacion FROM pv_pasarela_reseller_mvno_activations WHERE phoneNumber='$T_Msisdn'","pv_pasarela_reseller_mvno_activations","L"); 
     &consulta("mnet","SELECT Fecha_Solicitud, Hora_Solicitud, Tipo_Usuario, Id_Usuario, Id_Perfil_Producto, Msisdn, Numeracion_Reciclada, Id_Cliente FROM pv_solicitudes_productos WHERE Msisdn='$T_Msisdn'","pv_solicitudes_productos","L"); 
     &consulta("mnet","SELECT Fecha_Solicitud, Hora_Solicitud, Tipo_Usuario, Id_Usuario, Msisdn, Numeracion_Reciclada, Id_Cliente, Nombre FROM pv_solicitudes_datos_clientes WHERE Msisdn='$T_Msisdn'","pv_solicitudes_datos_clientes","L"); 
   }



 }
 return(0);
}








sub portabilidad

{

 my($T_Msisdn, $T_Id_Cliente)=@_;


 $opcion2="";

while ($opcion2 ne "99")

 {
 print "-------------------------------\n";
 print "| 6. Portabilidad             |\n";
 print "-------------------------------\n";
 print "  Msisdn: $T_Msisdn Prepago: $T_Id_Prepago ($T_Id_Prepago_Mvno)\n";
 print " 6.1 Consulta Solicitudes Alta Portabilidad\n";
 print " 6.2 Consulta Solicitudes Alta Portabilidad por tarjeta Iccid\n";
 print " 6.3 Consulta Transacciones Alta Portabilidad\n";
 print " 6.4 Consulta Transacciones Solicitudes Alta Portabilidad\n";
 print " 6.5 Consulta Transacciones Cambio Estado Portabilidad (requiere idSolicitud)\n";
 print "  99 Menu Principal\n";

 $opcion2= prompt("Dime que quieres hacer");

 if ($opcion2 eq "6.1") { &consulta("mnet","SELECT * FROM pv_solicitudes_alta_portabilidad WHERE Msisdn='$T_Msisdn'","pv_solicitudes_alta_portabildad","L"); }
 if ($opcion2 eq "6.2") { $iccid=prompt ("Dime iccid",$iccid); &consulta("mnet","SELECT * FROM pv_solicitudes_alta_portabilidad WHERE IccidNuevo='$iccid'","pv_solicitudes_alta_portabilidad","L"); }
 if ($opcion2 eq "6.3") { &consulta("mnet","SELECT * FROM pv_transacciones_alta_portabilidad WHERE Msisdn='$T_Msisdn'","pv_transacciones_alta_portabildad","L"); }
 if ($opcion2 eq "6.4") { &consulta("mnet","SELECT * FROM pv_transacciones_solicitudes_alta_portabilidad WHERE Msisdn='$T_Msisdn'","pv_transacciones_alta_portabildad","L"); }
 if ($opcion2 eq "6.5") { $idSolicitud=prompt ("Dime idSolicitud",$idSolicitud); &consulta("mnet","SELECT * FROM pv_transacciones_cambio_estado_portabilidad WHERE idSolicitud='$idSolicitud'","pv_transacciones_alta_portabildad","L"); }

 }
 return(0);
} # portabilidad











#01 Detalle en tablas

sub productos

{

 my($T_Msisdn, $T_Id_Cliente)=@_;


 $opcion2="";

while ($opcion2 ne "99")

 {
 print "-------------------------------\n";
 print "| 7. Detalle de tablas        |\n";
 print "-------------------------------\n";
 print "  Msisdn: $T_Msisdn Prepago: $T_Id_Prepago ($T_Id_Prepago_Mvno)\n";
 print " Promociones\n";
 print " 7.1 Consulta resumen de todas las Promociones\n";
 print " 7.2 Consulta detalle de todas las Promociones\n";
 print " 7.3 Consulta promociones en msisdn\n";
 print "Bonos\n"; 
 print " 7.4 Consulta los Bonos y Bonos adicionales en curso del Msisdn\n";
 print " 7.5 Crear Solicitud Bono\n";
 print " 7.6 Desactivar bono\n";
 print "Bonos Adicionales\n";
 print " 7.7 Activar un Bono Adicional\n";
 print "\n Cambios Producto\n"; 
 print " 7.9 Consulta Solicitudes Cambio\n";
 print " 7.10 Cancelacion Solicitudes Cambio\n";
 print "  99 Menu Principal\n";

 $opcion2= prompt("Dime que quieres hacer");

 if ($opcion2 eq "7.1") { &consulta("mvno","SELECT  Id_Promo, Id_Submvno, Descripcion, DATE_FORMAT(STR_TO_DATE(Activar_Auto_Fecha_Inicio,'%Y%m%d'),'%d/%m/%Y') AS Activacion_Fecha_Inicio, DATE_FORMAT(STR_TO_DATE(Activar_Auto_Fecha_Fin,'%Y%m%d'),'%d/%m/%Y') AS Activacion_Fecha_Fin, DATE_FORMAT(STR_TO_DATE(Fecha_Inicio,'%Y%m%d'),'%d/%m/%Y') AS Fecha_Inicio, DATE_FORMAT(STR_TO_DATE(Fecha_Fin,'%Y%m%d'),'%d/%m/%Y') AS Fecha_Fin FROM pv_mvno_promociones","pv_msisdn","G"); }
 if ($opcion2 eq "7.2") { &consulta("mvno","SELECT * FROM pv_mvno_promociones","pv_msisdn","L"); }
 if ($opcion2 eq "7.3") { &consulta("mvno","SELECT * FROM pv_mvno_promociones_msisdn WHERE Msisdn='$T_Msisdn'","pv_iccid","G"); }

 if ($opcion2 eq "7.4") { 
                          &consulta("mvno","SELECT * FROM pv_mvno_bonos_msisdn WHERE Msisdn='$T_Msisdn'","pv_mvno_bonos_msisdn","G"); 
                          &consulta("mvno","SELECT Id_Bono, Flag_Estado, Fecha_Inicio, Fecha_Fin,(Datos_Bytes_Max_Total/1024/1024) as MBT,(Datos_Bytes_Consumidos/1024/1024) as MBC FROM pv_mvno_bonos_adicionales WHERE Msisdn='$T_Msisdn' AND Id_cliente='$T_Id_Cliente' AND Fecha_Inicio<='$fecha' AND Fecha_Fin>='$fecha';","pv_mvno_bonos_msisdn","G"); 

                        }


 if ($opcion2 eq "7.7") {
                         $qry_bonos=qq{SELECT Bonos_Adicionales  
                                         FROM pv_mvno_perfiles_clientes
                                        WHERE Id_Producto='$T_Id_Producto'};
                         $st_bonos=$db_mvno->prepare($qry_bonos);
                         $st_bonos->execute();
                         ($que_bonos_adicionales)=$st_bonos->fetchrow_array();
                         $st_bonos->finish();

                         if ($que_bonos_adicionales eq "")
                           {
                            print "El producto del cliente $T_Id_Producto no tiene definidos bonos adicionales\n";
                           }
                           else
                           {
                            @bonos_adicionales=split(";",$que_bonos_adicionales);
                            $bonosin="";

                            foreach $erbono(@bonos_adicionales)
                                   {
                                    $bonosin=$bonosin."'$erbono',";
                                   }
                            chop($bonosin);

                            print "Bonos adicionales Disponibles:\n";

                            $qry_bonos=qq{SELECT Id_Bono, Descripcion, Precio_Bono FROM pv_mvno_bonos WHERE Id_Bono IN ($bonosin)};
                            $st_bonos=$db_mvno->prepare($qry_bonos);
                            $st_bonos->execute();
                            while ( ($er_Id_Bono, $T_Descripcion, $N_Precio_Bono)=$st_bonos->fetchrow_array() )
                                 {
                                  print " - $er_Id_Bono, $T_Descripcion - $N_Precio_Bono Euros\n";
                                 }
                            $st_bonos->finish();
                           }

                           $T_Id_Bono=prompt ("Dime Id_Bono a Activar");
                           $T_Provisionado_Observaciones= prompt ("Dime el motivo de activacion:",$T_Provisionado_Observaciones);          
                           $qry_activa=qq{INSERT INTO pv_solicitudes_bonos_adicionales SET 
                                                Fecha='$fecha',
                                                Hora='$hora',
                                                Tipo_Solicitud ='A',
                                                Tipo_usuario='T',
                                                Id_Usuario='miramsisdn',
                                                Msisdn         ='$T_Msisdn',
                                                Id_Cliente     ='$T_Id_Cliente',
                                                Id_Cliente_Fact='$T_Id_Cliente_Fact',
                                                Flag_Realizado='N',
                                                Id_Bono='$T_Id_Bono',
                                                Provisionado_Observaciones='$T_Provisionado_Observaciones'};
                           $st_activa=$db_mnet->prepare($qry_activa);
                           $st_activa->execute();
                           $st_activa->finish();
                           print $qry_activa."\n";
                           print "Solicitud de bono realizada.\n\n";
                           $opcion2="99";

                        }


 
 if ($opcion2 eq "7.5") { 
 	
                 print "Bonos compatibles con el producto $T_Id_Producto\n";
                 &consulta("mvno","SELECT Id_Bono AS Bono, (SELECT Descripcion FROM pv_mvno_bonos WHERE Id_Bono=Bono ORDER BY Fecha_Fin DESC LIMIT 1) AS Descripcion from pv_mvno_productos_bonos where id_producto='$T_Id_Producto' AND Flag_Compatible='S'","pv_mvno_cambio_producto_msisdn","G"); 
	
 		 $T_Id_Bono=prompt ("Dime Id_Bono a Activar",$T_Id_Bono);
 		 #Habria que controlar si ya tiene el bono en cuestion activado
 		 $T_Tipo_usuario=prompt ("Dime Tipo Usuario (T para B0023)",$T_Tipo_usuario);
 		 $T_Provisionado_Observaciones= prompt ("Dime el motivo de activacion:",$T_Provisionado_Observaciones);		 
                 $fecha_mes_siguiente=prompt("Dime la fecha prevista de activacion (recomendable dia 1 de mes siguiente)",$fecha_mes_siguiente);

 		 $qry_activa=qq{INSERT INTO mnet.pv_solicitudes_bonos SET 
                                    Tipo_Solicitud    ='A',
  				    msisdn            ='$T_Msisdn',
 		 		    Id_Cliente        ='$T_Id_Cliente',
 		 	            Id_Cliente_Fact   ='$T_Id_Cliente_Fact',
 		 		    Flag_Realizado    ='P',
 		 		    Fecha_realizacion ='$fecha_mes_siguiente',
 		 		    Fecha             ='$fecha',
                                    Hora              ='$hora',
                                    Tipo_usuario      ='$T_Tipo_usuario',
                                    Id_Bono           ='$T_Id_Bono',
                                    Id_Usuario        ='miramsisdn',
                                    Provisionado_Observaciones='$T_Provisionado_Observaciones'};
                          $st_activa=$db_mnet->prepare($qry_activa);
                          $st_activa->execute();
                          $st_activa->finish();
                          print " --- Se ha creado la solicitud de bono ---"."\n";
                          $opcion2="99";
 	}#Fin opcion 7.5
	if ($opcion2 eq "7.6") { 
 		
 		 $T_Id_Bono=prompt ("Dime Id_Bono a Desactivar",$T_Id_Bono);
                 $fecha_mes_siguiente=prompt("Dime la fecha prevista de la desactivacion (recomendable dia 1 de mes siguiente)",$fecha_mes_siguiente);
 		 #Habria que controlar si ya tiene el bono en cuestion activado
 		 $T_Tipo_usuario=prompt ("Dime Tipo Usuario (T para B0023)",$T_Tipo_usuario);
 		 $T_Provisionado_Observaciones= prompt ("Dime el motivo de desactivacion:",$T_Provisionado_Observaciones);		 
 		 $qry_activa=qq{INSERT INTO mnet.pv_solicitudes_bonos SET 
                                    Tipo_Solicitud='B',
 		 		    msisdn='$T_Msisdn',
 		 		    Id_Cliente='$T_Id_Cliente',
 		 		    Id_Cliente_Fact='$T_Id_Cliente_Fact',
 		 		    Flag_Realizado='P',
 		 		    Fecha_realizacion='$fecha_mes_siguiente',
 		 		    Fecha='$fecha',
                                    Hora='$hora',
                                    Tipo_usuario='$T_Tipo_usuario',
                                    Id_Bono='$T_Id_Bono',
                                    Id_Usuario='miramsisdn',
                                    Provisionado_Observaciones='$T_Provisionado_Observaciones'};
                          $st_activa=$db_mnet->prepare($qry_activa);
                          $st_activa->execute();
                          $st_activa->finish();
                          print " --- Se ha creado la solicitud de desactivacion bono ---"."\n";
                          $opcion2="99";
 	}#Fin opcion 7.6
	

	if ($opcion2 eq "7.9") { &consulta("mnet","SELECT Id_Solicitud,Fecha_Solicitud,Hora_Solicitud,Msisdn,Id_Cliente,Fecha,Id_Tipo_Cambio,Flag_Realizado,Bonos_Activar,Realizado_Observaciones   FROM pv_mvno_cambio_producto_msisdn WHERE Msisdn='$T_Msisdn'","pv_mvno_cambio_producto_msisdn","G"); }
	if ($opcion2 eq "7.10") {
		$T_Id_Solicitud=prompt ("Dime Id_Solicitud a Cancelar",$T_Id_Solicitud);
		$qry_sol_cambio=qq{SELECT Id_Solicitud,Msisdn,Id_Cliente,Fecha,Id_Tipo_Cambio FROM mnet.pv_mvno_cambio_producto_msisdn WHERE Id_Solicitud='$T_Id_Solicitud'};
        $st_sol_cambio=$db_mvno->prepare($qry_sol_cambio);
        $st_sol_cambio->execute();
        ($T_Id_Solicitud_cance,$T_Msisdn_cance,$T_Id_Cliente_cance,$T_Fecha_cance,$T_Id_Tipo_Cambio_cance)=$st_sol_cambio->fetchrow_array();
        $st_sol_cambio->finish();
		
		$error="";
		if($T_Id_Solicitud ne $T_Id_Solicitud_cance){$error="Error no existe la solicitud indicada"; print "$error\n";}		
		if($error eq ""){
			print "Solicitud a cancelar $T_Id_Solicitud_cance,$T_Msisdn_cance,$T_Id_Cliente_cance,$T_Fecha_cance,$T_Id_Tipo_Cambio_cance\n\n";
			$motivo_cancelacion=prompt("Dime el motivo:\n");
			$confirmacion_cancelacion=prompt("Confirmas cancelacion cambio $T_Id_Solicitud $motivo_cancelacion (s/n)");			
			if ($confirmacion_cancelacion eq "s"){
								$qry_cancelacion=qq{UPDATE mnet.pv_mvno_cambio_producto_msisdn SET 
													Flag_Realizado='X',
													Realizado_Observaciones='$motivo_cancelacion'
													WHERE Id_Solicitud='$T_Id_Solicitud'};
									$st_cancelacion=$db_mnet->prepare($qry_cancelacion);
									$st_cancelacion->execute();
									$st_cancelacion->finish();
									print $qry_cancelacion."\n";	 
			}#if ($confirmacion_cancelacion eq "s")
		}#if($error eq "")		
		$opcion2="7.8"	
	}#if ($opcion2 eq "7.9") 
	
 }
 return(0);
} # productos





sub migracion_perfil_electrico

{

 my($T_Msisdn, $T_Id_Cliente)=@_;


 $opcion2="";

while ($opcion2 ne "99")

 {
 print "-------------------------------------------------\n";
 print "| 12. Migracion Perfil Electrico y cambio de SIM|\n";
 print "-------------------------------------------------\n";
 print "  Msisdn: $T_Msisdn Pago Producto:$T_Id_Prepago  Pago Mvno:$T_Id_Prepago_Mvno\n";
 print " Cambio de Perfil Electrico\n";
 print " 12.1 Migracion de Prepago a Postpago\n";
 print " 12.2 Consulta Solicitudes de Migracion de Prepago a Postpago\n";
 print " 12.3 Migracion de Postpago a Prepago\n";
 print " 12.4 Consulta Solicitudes de Migracion de Postpago a Prepago\n";
 print " 12.5 Cambio de SIM\n";
 print "  99 Menu Principal\n";

 $opcion2= prompt("Dime que quieres hacer");

 if ($opcion2 eq "12.1") 
   {
    if ($T_Id_Prepago_Mvno ne "P")
      {
       print "No puedo Migrar. Para una migracion a perfil electrico Postpago, es necesario que el perfil electrico este como Prepago (P)\n";
      }
      else
      {
       $motivo_cambio=prompt("Dime el motivo del cambio de perfil");
       $la_respuesta=&msisdnCambioPerfilElectrico($T_Msisdn,$N_Numeracion_Reciclada,$T_Id_Cliente,$T_Id_Cliente_Fact,"T","miramsisdn.pl","MIGRACIONPREPAGOPOSTPAGO",$motivo_cambio);
       print "Respuesta: $la_Respuesta.\n(ten en cuenta que el cambio tardara unos minutos en procesarse. Revisa el punto 12.2 para ver si todo ha ido bien.\n";
      }
    }

if ($opcion2 eq "12.2")
   {
    &consulta("mnet","SELECT Id_Solicitud, fechaSolicitud, MSISDN, statusServidor, errorServidor, fecha, hora, horaClienteResponse, Id_Estado FROM pv_solicitudes_prepago_postpago WHERE Msisdn='$T_Msisdn'","pv_msisdn","G");
   }
if ($opcion2 eq "12.3") 
   {
    if ($T_Id_Prepago_Mvno ne "T")
      {
       print "No puedo Migrar. Para una migracion a perfil electrico Prepago, es necesario que el perfil electrico este como Postpago (T)\n";
      }
      else
      {
       $motivo_cambio=prompt("Dime el motivo del cambio de perfil");
       $la_respuesta=&msisdnCambioPerfilElectrico($T_Msisdn,$N_Numeracion_Reciclada,$T_Id_Cliente,$T_Id_Cliente_Fact,"P","miramsisdn.pl","MIGRACIONPOSTPAGOPREPAGO",$motivo_cambio);
       print "Respuesta: $la_Respuesta.\n(ten en cuenta que el cambio tardara unos minutos en procesarse. Revisa el punto 12.4 para ver si todo ha ido bien.\n";
      }
    }
if ($opcion2 eq "12.4")
   {
    &consulta("mnet","SELECT Id_Solicitud, fechaSolicitud, MSISDN, statusServidor, errorServidor, fecha, hora, horaClienteResponse, Id_Estado FROM pv_solicitudes_postpago_prepago WHERE Msisdn='$T_Msisdn'","pv_msisdn","G");
   }

 if ($opcion2 eq "12.5")
   {
    if ($T_Id_Estado_Actual eq  "B" || $T_Id_Estado_Actual eq "P")
      {
       print "Para reemplazar la sim debe estar activada. Ahora esta en estado $T_Id_Estado_Actual\n";
      }
      else
      {
       &consulta("mnet","SELECT Id_Motivo, Descripcion FROM pv_cambio_sim_motivos","pv_cambio_sim_motivos","G");
 
       $error="";

       $motivo_cambio=prompt("Dime el motivo del cambio de perfil");
       $newiccid     =prompt("Dime el nuevo iccid");
       $observaciones=prompt("Observaciones");

       $qry_ver_iccid=qq{SELECT Id_producto FROM pv_iccid WHERE Iccid='$newiccid'};
       $st_ver_iccid=$db_mvno->prepare($qry_ver_iccid);
       $st_ver_iccid->execute();
       ($er_new_producto_iccid)=$st_ver_iccid->fetchrow_array();
       $st_ver_iccid->finish();
 
       $qry_ver_perfil=qq{SELECT Id_Producto_CambioSim FROM pv_mvno_perfiles_clientes WHERE Id_Producto='$T_Id_Producto'};
       $st_ver_perfil=$db_mvno->prepare($qry_ver_perfil);
       $st_ver_perfil->execute();
       ($er_producto_cambio_sim)=$st_ver_perfil->fetchrow_array();
       $st_ver_perfil->finish();
        
       if ($motivo_cambio eq "") {$error="Por favor, dime el motivo del cambio de perfil";} 
       if ($er_new_producto_iccid eq "")  {$error="Error en iccid $newiccid. No existe";}
      #  if ($er_new_producto_iccid ne $er_producto_cambio_sim) {$error="El cambio de sim deber ser en tarjetas $er_producto_cambio_sim y el nuevo sim tiene el producto $er_new_producto_iccid";}

       if ($error eq "")
         {      
           $qry_cambio_sim=qq{INSERT INTO pv_cambio_sim SET
                              Id_Agente   ='',
                              Id_usuario  ='admin',
                              Fecha       ='$fecha',
                              Hora        ='$hora',
                              Id_Motivo_Cambio ='$motivo_cambio',
                              MSISDN_Origen    ='$T_Msisdn',
                              Id_Cliente       ='$T_Id_Cliente',
                              ICCID_Origen     ='$T_Iccid',
                              ICCID_Reemplazo  ='$newiccid',
                              Id_Estado        ='P',
                              Observaciones    ='$observaciones'};
           $st_cambio_sim=$db_mnet->prepare($qry_cambio_sim);
           $st_cambio_sim->execute();
           $st_cambio_sim->finish();
           print $qry_cambio_sim."\n";
         }
         else
         {
           print $error."\n\n";
         }

      }
    }




 } # while ($opcion2 ne "99")


} # sub migracion_perfil_electrico




sub ciclo_vida_prepago

{


 my($T_Msisdn, $T_Id_Cliente)=@_;


 $opcion2="";

while ($opcion2 ne "99")

 {
 print "-----------------------------\n";
 print "| 10. Ciclo de Vida Prepago |\n";
 print "-----------------------------\n";
 print "  Msisdn: $T_Msisdn Prepago: $T_Id_Prepago ($T_Id_Prepago_Mvno)\n";
 print " 10.1 Activa un Msisdn en estado Preactivado\n";
 print " 10.2 Bloquear por Riesgo Misdn\n";
 print " 10.3 Desbloquear por Riesgo Misdn\n";
 print " 10.4 Bloquear por no identificacion (estado F)\n";
 print " 10.5 Logs de pv_transacciones_prepago\n";
 print " 10.6 Bloqueo temporal Fraude L\n";
 print "   99 Menu Principal\n";

 $opcion2= prompt("Dime que quieres hacer");

 if ($opcion2 eq "10.1") {
                          $respuesta=&msisdnCambioEstadoRed($T_Msisdn,$T_Id_Prepago_Mvno,$N_Numeracion_Reciclada,$T_Id_Cliente, $T_Id_Cliente_Fact,"T","miramsisdn","A","Solicitud miramsisdn");
                          print $respuesta."\n";
                          $opcion2="99";
                         }

 if ($opcion2 eq "10.2") { 
                          $respuesta=&msisdnCambioEstadoRed($T_Msisdn,$T_Id_Prepago_Mvno,$N_Numeracion_Reciclada,$T_Id_Cliente, $T_Id_Cliente_Fact,"T","miramsisdn","R","Solicitud miramsisdn");
                          print $respuesta."\n";
                          $opcion2="99";
                         }

 if ($opcion2 eq "10.3") { 
                          $respuesta=&msisdnCambioEstadoRed($T_Msisdn,$T_Id_Prepago_Mvno,$N_Numeracion_Reciclada,$T_Id_Cliente, $T_Id_Cliente_Fact,"T","miramsisdn","U","Solicitud miramsisdn");
                          print $respuesta."\n";
                          $opcion2="99";
                         }

 if ($opcion2 eq "10.4") { 
                          $respuesta=&msisdnCambioEstadoRed($T_Msisdn,$T_Id_Prepago_Mvno,$N_Numeracion_Reciclada,$T_Id_Cliente, $T_Id_Cliente_Fact,"T","miramsisdn","F","Solicitud miramsisdn");
                          print $respuesta."\n";
                          $opcion2="99";
                         }
if ($opcion2 eq "10.6") {
                          #$respuesta=&msisdnCambioEstadoRed($T_Msisdn,$T_Id_Prepago_Mvno,$N_Numeracion_Reciclada,$T_Id_Cliente, $T_Id_Cliente_Fact,"T","miramsisdn","L","Solicitud miramsisdn");
                          #print $respuesta."\n";
                          $qry_bloqueo=qq{INSERT INTO pt_msisdn_actualiza SET MSISDN ='$T_Msisdn', Id_Accion='L' ON DUPLICATE KEY UPDATE Msisdn='$T_Msisdn', Id_Accion ='L'};
			 $st_bloqueo=$db_mvno->prepare($qry_bloqueo);
          		 $st_bloqueo->execute();
          		 $st_bloqueo->finish();
                          $opcion2="99";
                         }

  if ($opcion2 eq "15.2") {print "\n";
                        if($T_Id_Cliente eq ''){
                        	$T_Id_Cliente =prompt("Dime numero cliente:");
                        } 
						print "Motivo de baja para el cliente:\n\n";
						print " 1. Baja solicitada por cliente\n";
						print " 2. Baja solicitada por Impago\n";									
						print "   99 Salir\n\n";
						
						$opcionBaja="";
                        $opcionBaja=prompt("Elije la opcion de baja\n");
                        $bajaDescrip="";
                        if($opcionBaja ne "99"){
	                        	if($opcionBaja eq "1"){
	                        		$bajaDescrip="Baja solicitada por cliente ";
	                        		$Id_Departamento="MVNO-CALLCENTER";
	                        	}
	                        	if($opcionBaja eq "2"){
	                        		$bajaDescrip="Baja solicitada por Impago ";
	                        		$Id_Departamento="FINANCIERO";
	                        	}
	                        	$motivo_baja=prompt("Dime el motivo:\n");
	                        	$confirmacion=prompt("Confirmas $bajaDescrip $motivo_baja (s/n)");
                        	}						

                        if ($confirmacion eq "s"){
                        	$qry_activa=qq{UPDATE xprv.pv_clientes SET Id_Estado_Cliente='400',
                        						Id_Sub_Estado_Cliente='00',
									Fecha_Id_Estado='$fecha' 
                        						WHERE Id_Cliente='$T_Id_Cliente'};
                        		$st_activa=$db_xprv->prepare($qry_activa);
                             	$st_activa->execute();
                            	$st_activa->finish();
                             	print $qry_activa."\n";
                        
                        	 $qry_nota=qq{INSERT INTO xprv.pv_clientes_notas SET Id_Cliente='$T_Id_Cliente',
                        	 				Fecha_Nota='$fecha',
                        	 				Hora_Nota='$hora',
                        	 				Id_Departamento='$Id_Departamento',
                        	 				Id_Usuario='miramsisdn',
                        	 				Descripcion='$bajaDescrip $motivo_baja'};                      	 
                        	
	                             $st_nota=$db_xprv->prepare($qry_nota);
	                             $st_nota->execute();
	                             $st_nota->finish();
	                             print $qry_nota."\n";
                        	}                            
                          $opcion2="99"
                         }
                         

 if ($opcion2 eq "10.5") { &consulta("mnet","SELECT * FROM pv_transacciones_prepago WHERE Msisdn='$T_Msisdn'","pv_transacciones_prepago","L"); }

 }




 return(0);





} # ciclo_vida_prepago



sub ciclo_vida_postpago

{


 my($T_Msisdn, $T_Id_Cliente)=@_;


 $opcion2="";

while ($opcion2 ne "99")

 {
 print "-----------------------------\n";
 print "| 11. Ciclo de Vida Postpago |\n";
 print "-----------------------------\n";
 print "  Msisdn: $T_Msisdn Prepago: $T_Id_Prepago ($T_Id_Prepago_Mvno)\n";
 print " 11.1 Consulta limite de consumo actual (control de riesgo)\n";
 print " 11.2 Cambio limite de consumo (control de riesgo)\n";
 print " 11.3 Suspension de linea                    (A -> S)\n";
 print " 11.4 Reactivacion de Linea                  (S -> A)\n";
 print " 11.5 Call Barring                           (A -> J)\n";
 print " 11.6 Hot Line                               (A -> H)\n";
 print " 11.7 Desbloqueo Call Barring,HotLine Fraude (J,H,F -> A)\n";
 print " 11.8 Suspension de Linea con Desactivacion de Bonos\n";
 print " 11.9 Bloqueo por Fraude                     (A -> F)\n";
 print "   99 Menu Principal\n";

 $opcion2= prompt("Dime que quieres hacer");

 if ($opcion2 eq "11.1") { &consulta("mvno","SELECT * FROM pv_mvno_riesgo_msisdn WHERE Msisdn='$T_Msisdn' AND Id_Cliente='$T_Id_Cliente'","pv_mvno_riesgo_msisdn","L"); }
 if ($opcion2 eq "11.2") {
 	                  if ($T_Id_Prepago eq "P")#$T_Id_Prepago ne "T" || 
                            {
                             print "No puedo cambiar limite de riesgo. No tiene perfil postpago.\n";
                            }
                             else
                            {	
      			     $qry_Limite=qq{SELECT Riesgo_Mensual 
			                      FROM pv_mvno_riesgo_msisdn
			                     WHERE Msisdn='$T_Msisdn' AND Id_Cliente='$T_Id_Cliente'};
			     $st_Limite=$db_mvno->prepare($qry_Limite);
			     $st_Limite->execute();
			     ($N_Riesgo_Mensual)=$st_Limite->fetchrow_array();
			     $st_Limite->finish();
			     print "El limite de riesgo actual es:".$N_Riesgo_Mensual."\n";
			
			     $Riesgo_Mensual=prompt("Dime la cantidad limite",$Riesgo_Mensual);
                             $que_motivo=prompt("Dime el motivo del cambio de limite:",$que_motivo);
                             if($N_Riesgo_Mensual eq ""){              
		                                         $qry_activa=qq{INSERT INTO pv_mvno_riesgo_msisdn SET 
		              				       	          MSISDN ='$T_Msisdn', 
		              					          Id_cliente='$T_Id_Cliente',
		              					          Riesgo_Mensual=$Riesgo_Mensual};
				                         }else{
					                 $qry_activa=qq{UPDATE pv_mvno_riesgo_msisdn SET 
	              					                  MSISDN ='$T_Msisdn', 
	              					                  Id_cliente='$T_Id_Cliente',
	              					                  Riesgo_Mensual=$Riesgo_Mensual
	              					        WHERE Msisdn='$T_Msisdn' AND Id_Cliente='$T_Id_Cliente'};
				
				                        }#if($N_Riesgo_Mensual eq "") 
                             $st_activa=$db_mvno->prepare($qry_activa);
                             $st_activa->execute();
                             $st_activa->finish();
              
                             $qry_log=qq{INSERT INTO log_msisdn SET
                                           Msisdn    ='$T_Msisdn',
                                           Id_Cliente='$T_Id_Cliente',
                                           Id_Cliente_Fact='$T_Id_Cliente_Fact',
                                           Tipo_Usuario ='X',
                                           Id_Usuario='Thor',
                                           Fecha='$fecha',
                                           Hora='$hora',
                                           Id_Evento='Postpago',
                                           Id_Subevento='Limite_Riesgo_Mensual', 
                                           Descripcion='$que_motivo'};
                             $st_log=$db_mlog->prepare($qry_log);
                             $st_log->execute();
                             $st_log->finish();
              
                             print $qry_activa."\n";
                             $opcion2="99";
                             }
                           } # if ($opcion2 eq "11.2")

 if ($opcion2 eq "11.3") {
                          $qry_activa=qq{INSERT INTO pt_msisdn_actualiza_postpago SET MSISDN='$T_Msisdn', Id_Accion='S' ON DUPLICATE KEY UPDATE MSISDN='$T_Msisdn', Id_Accion='S'};
                          $st_activa=$db_mvno->prepare($qry_activa);
                          $st_activa->execute();
                          $st_activa->finish();
                          print $qry_activa."\n";
                          $opcion2="99";
                         }

 if ($opcion2 eq "11.4") {
                          $qry_activa=qq{INSERT INTO pt_msisdn_actualiza_postpago SET MSISDN='$T_Msisdn', Id_Accion='R' ON DUPLICATE KEY UPDATE MSISDN='$T_Msisdn', Id_Accion='R'};
                          $st_activa=$db_mvno->prepare($qry_activa);
                          $st_activa->execute();
                          $st_activa->finish();
                          print $qry_activa."\n";
                          $opcion2="99";
                         }



 if ($opcion2 eq "11.5") {
                          $qry_activa=qq{INSERT INTO pt_msisdn_actualiza_postpago SET MSISDN='$T_Msisdn', Id_Accion='J' ON DUPLICATE KEY UPDATE MSISDN='$T_Msisdn', Id_Accion='J'};
                          $st_activa=$db_mvno->prepare($qry_activa);
                          $st_activa->execute();
                          $st_activa->finish();
                          print $qry_activa."\n";
                          $opcion2="99";
                         }


 if ($opcion2 eq "11.6") {
                          $qry_activa=qq{INSERT INTO pt_msisdn_actualiza_postpago SET MSISDN='$T_Msisdn', Id_Accion='H' ON DUPLICATE KEY UPDATE MSISDN='$T_Msisdn', Id_Accion='H'};
                          $st_activa=$db_mvno->prepare($qry_activa);
                          $st_activa->execute();
                          $st_activa->finish();
                          print $qry_activa."\n";
                          $opcion2="99";
                         }
             
 if ($opcion2 eq "11.9") {
                          $qry_activa=qq{INSERT INTO pt_msisdn_actualiza_postpago SET MSISDN='$T_Msisdn', Id_Accion='F', Id_Motivo='Fraude' ON DUPLICATE KEY UPDATE MSISDN='$T_Msisdn', Id_Accion='F', Id_Motivo='Fraude'};
                          $st_activa=$db_mvno->prepare($qry_activa);
                          $st_activa->execute();
                          $st_activa->finish();
                          print $qry_activa."\n";
                          $opcion2="99";
                         }



if ($opcion2 eq "11.7") {
                          $qry_activa=qq{INSERT INTO pt_msisdn_actualiza_postpago SET MSISDN='$T_Msisdn', Id_Accion='D' ON DUPLICATE KEY UPDATE MSISDN='$T_Msisdn', Id_Accion='D'};
                          $st_activa=$db_mvno->prepare($qry_activa);
                          $st_activa->execute();
                          $st_activa->finish();
                          print $qry_activa."\n";
                          $opcion2="99";
                         }
 if ($opcion2 eq "11.8") {
 						  $descripcion=prompt("Dime el motivo de la Suspesion y Baja de los Bonos:");
                          $qry_activa=qq{INSERT INTO pt_msisdn_actualiza_postpago SET MSISDN='$T_Msisdn', Id_Accion='S' ON DUPLICATE KEY UPDATE MSISDN='$T_Msisdn', Id_Accion='S'};
                          $st_activa=$db_mvno->prepare($qry_activa);
                          $st_activa->execute();
                          $st_activa->finish();
                          print $qry_activa."\n";
                          &baja_bonos;
                          $opcion2="99";
                         }		
                          
 

 
 }# while ($opcion2 ne "99")

} # ciclo_vida_postpago


sub facilidades_red

{

 my($T_Msisdn, $T_Id_Cliente)=@_;


 $opcion2="";

while ($opcion2 ne "99")

 {
 print "----------------------------------------\n";
 print "| 13. Facilidades de Red (MVNO)|\n";
 print "----------------------------------------\n";
 print "  Msisdn: $T_Msisdn Pago Producto:$T_Id_Prepago  Pago Mvno:$T_Id_Prepago_Mvno\n";
 print " 13.1 Consulta Facilidades Red Msisdn\n";
 print " 13.2 Consulta Solicitudes de Facilidades Red Msisdn\n";
 print " 13.3 Administracion de Facilidades Red Msisdn\n";
 print "   99 Menu Principal\n";

 $opcion2= prompt("Dime que quieres hacer");

 if ($opcion2 eq "13.1") {
    					$qry_activa=qq{INSERT INTO pt_msisdn_consulta_facilidades_red SET MSISDN='$T_Msisdn', Id_Accion='C'};
                        $st_activa=$db_mvno->prepare($qry_activa);
                        $st_activa->execute();
                        $st_activa->finish();
                        print $qry_activa."\n";
                        $opcion2="13";
    					}#fin 13.1

if ($opcion2 eq "13.2"){
    					&consulta("mvno","SELECT MSISDN, fecha, hora, Identificacion_De_Numero, Desvio_Incondicional, Desvio_Incondicional_Destino, Desvio_Incondicional_MSISDN, Desvio_Si_Ocupado, Desvio_Si_Ocupado_Destino, Desvio_Si_Ocupado_MSISDN,Desvio_Si_No_Contesta, Desvio_Si_No_Contesta_Destino, Desvio_Si_No_Contesta_MSISDN,Desvio_Si_No_Accesible,Desvio_Si_No_Accesible_Destino,Desvio_Si_No_Accesible_MSISDN,Aviso_Llamadas, Bloqueo_Llamadas_Entrantes,Bloqueo_Llamadas_Salientes,Retencion_Llamadas, Llamada_En_Espera, Multiconferencia,Envio_SMSs  FROM pv_facilidades_red_msisdn WHERE MSISDN='$T_Msisdn'","pv_msisdn","L");
   						}#fin 13.2
   
if ($opcion2 eq "13.3"){
						print "\n";
						print "Facilidades de Red:\n\n";
						print " 1. Identificacion_De_Numero\n";
						print " 2. Aviso_Llamadas\n";
						print " 3. Bloqueo_Llamadas_Entrantes\n";
						print " 4. Bloqueo_Llamadas_Salientes | Deshabilitado\n";						
						print " 5. Retencion_Llamadas\n";
						print " 6. Llamada_En_Espera\n";						
						print " 7. Aviso_Disponibilidad | Deshabilitado\n";
						print " 8. Envio_SMSs\n";
						print " 9. Buzon_De_Voz\n";						
						print "   99 Salir\n\n";
						$sqlFacilidad="";
						$opcionFacilidad="";
                        $opcionFacilidad=prompt("Elije la Facilidad de Red a Modificar\n");
                        
                        if($opcionFacilidad ne "99"){
                        	$valorFacilidad=prompt("Dime el valor para el cambio: SI/NO\n");
                        }

						use Switch;					
						switch ($opcionFacilidad) {
						  case "1" { $sqlFacilidad="Identificacion_De_Numero"; }
						  case "2" { $sqlFacilidad="Aviso_Llamadas"; }
						  case "3" { $sqlFacilidad="Bloqueo_Llamadas_Entrantes"; }
						  case "5" { $sqlFacilidad="Retencion_Llamadas"; }
						  case "6" { $sqlFacilidad="Llamada_En_Espera"; }
						  case "8" { $sqlFacilidad="Envio_SMSs"; }
						  case "9" { $sqlFacilidad="Desvio_Si_Ocupado='$valorFacilidad',Desvio_Si_No_Contesta='$valorFacilidad',Desvio_Si_No_Accesible"; }
						  case "99" { $opcion2="13"; }
						  default { print "Opcion Deshabilitada\n"; }
						}#switch 
                        if($sqlFacilidad ne ""){
                        	$qry_activa="";

                                 $q_id_estado="P";
                                 if ($T_Msisdn eq "627549229" || $T_Msisdn eq "691900014") { $q_id_estado="T"; }

                        	$qry_activa=qq{INSERT INTO pv_solicitudes_facilidades_red_msisdn SET 
                        					MSISDN='$T_Msisdn', 
                        					Id_Estado='$q_id_estado',
                        					Id_Usuario='miramsisdn',
                        					fecha='$fecha',
                        					hora='$hora',
                        					$sqlFacilidad = '$valorFacilidad'
                        					};
		                   	$st_activa=$db_mnet->prepare($qry_activa);
		                   	$st_activa->execute();
		                   	$st_activa->finish();
		                   	print $qry_activa."\n";
		                   	
                        }#if ($sqlFacilidad ne "")                           					
                        $opcion2="13";
    					}#fin 13.3

 } # while ($opcion2 ne "99")

} # sub facilidades_red






sub retarificaciones

{

 my($T_Msisdn, $T_Id_Submvno, $T_Old_Id_Cliente, $T_Old_Id_Cliente_Fact)=@_;


 $opcion2="";

 $fecha_log=$fecha;

while ($opcion2 ne "99")

 {
 print "--------------------------------------------------------\n";
 print "| 14. Cambios de Razpn Social y Retarificaciones        |\n";
 print "--------------------------------------------------------\n";
 print "  Msisdn: $T_Msisdn Prepago: $T_Id_Prepago ($T_Id_Prepago_Mvno)\n";
 print " 14.1 Cambio Id_Cliente e Id_Cliente_Fact del Msisdn $T_Msisdn.\n";
 print " 14.9 Consulta las solicitudes de cambio de razon social\n";
 print "   99 Menu Principal\n";

 print "\n";

 $opcion2= prompt("Dime que quieres hacer");



 if ($opcion2 eq "14.1") {
                          $new_id_cliente=prompt("Nuevo Codigo de cliente a Asignar:");
                          # Verificamos el cliente
                          #
                          $qry_v_cliente=qq{SELECT Id_Cliente, Id_Cliente_Fact, Nombre, Id_Grupo_Cliente AS IDGRUPO, 
                                                   (SELECT Id_Submvno FROM pv_clientes_grupos WHERE Id_Grupo_Cliente=IDGRUPO and id_empresa='05') 
                                              FROM pv_clientes 
                                             WHERE Id_Cliente='$new_id_cliente'};
                          $st_v_cliente=$db_xprv->prepare($qry_v_cliente);
                          $st_v_cliente->execute();
                          ($New_Id_Cliente, $New_Id_Cliente_Fact, $New_Nombre, $New_Id_Grupo_Cliente, $New_Id_Submvno)=$st_v_cliente->fetchrow_array();
                          $st_v_cliente->finish();

                          if ($New_Id_Cliente eq "")
                            {
                             print "Cliente $new_id_cliente NO encontrado!\n";
                            }
                            else
                            {
                             print "$New_Id_Cliente, $New_Id_Cliente_Fact, $New_Nombre, $New_Id_Submvno\n";
				#$New_Id_Submvno=$T_Id_Submvno;
                             if ($New_Id_Submvno ne $T_Id_Submvno)
                               {
                                print "Atencion. El Msisdn tiene el mvno $T_Id_Submvno, pero el cliente indicado tiene $New_Id_Submvno. No podemos hacer el cambio.\n\n";
                               }
                               else
                               {
                                $desde_fecha=prompt("Indica la fecha (YYYYMMDD) desde la que movemos el codigo de cliente");
                                
                                $conf_mov=prompt("Movemos el msisdn $T_Msisdn del cliente $T_Old_Id_Cliente al $New_Id_Cliente desde la fecha $desde_fecha? (s/n)");

                                if ($conf_mov eq "s" || $conf_mov eq "S")
                                  {
				
                                    $qry_cambio=qq{INSERT INTO pv_solicitudes_cambia_datos_msisdn SET 
                                                     Flag_Cambia_Perfil         ='N', 
                                                     Flag_Cambia_Cliente        ='S', 
                                                     Fecha                      ='$fecha', 
                                                     Hora                       ='$hora', 
                                                     Tipo_usuario               ='T', 
                                                     Id_Usuario                 ='root', 
                                                     Msisdn                     ='$T_Msisdn', 
                                                     Old_Id_Cliente             ='$T_Old_Id_Cliente', 
                                                     Old_Id_Cliente_Fact        ='$T_Old_Id_Cliente_Fact', 
                                                     Cambia_Cliente_Desde_Fecha ='$desde_fecha', 
                                                     New_Id_Cliente             ='$New_Id_Cliente', 
                                                     New_Id_Cliente_Fact        ='$New_Id_Cliente_Fact', 
                                                     Flag_Cambia_Id_Producto    ='N', 
                                                     Flag_Realizado             ='N'; 
                                                  };
                                    $st_cambio=$db_mnet->prepare($qry_cambio);
                                    $st_cambio->execute();
                                    $st_cambio->finish();

                                    print $qry_cambio."\n";

                                    print "Solicitud de cambio realizado.Estara en unos momentos\n\n";

                                  }
                                  else
                                  {
                                   print "Proceso CANCELADO!!!\n\n";
                                  }
                               }

                            }

                         } #  if ($opcion2 eq "14.1") 

 if ($opcion2 eq "14.9") { &consulta("mnet","SELECT Fecha, Hora, Old_Id_Cliente, New_Id_Cliente, Flag_Realizado, Realizado_Observaciones FROM pv_solicitudes_cambia_datos_msisdn WHERE Msisdn='$T_Msisdn' AND Flag_Cambia_Cliente='S' ","pv_solicitudes_cambia_datos_msisdn","L"); }




 }
 return(0);




}  # retarificaciones 




sub bajas

{

 my($T_Msisdn, $T_Id_Cliente, $T_Id_Cliente_Fact, $T_Id_Submvno)=@_;


 $opcion2="";

while ($opcion2 ne "99")

 {
 print "----------------------------------------\n";
 print "| 15. Bajas MVNO\n";
 print "----------------------------------------\n";
 print "  Msisdn: $T_Msisdn Pago Producto:$T_Id_Prepago  Pago Mvno:$T_Id_Prepago_Mvno\n";
 print "  Aviso: Las bajas de msisdn iran a la tabla mnet.pv_mvno_solicitudes_baja_msisdn\n";
 print "         La linea no se dara de baja inmediatamente, sino que se suspendera temporalmente\n";
 print "         y se dara de baja definitivamente transcurridos 7 dias.\n";
 print "  -------------------------------------------------------------\n\n";
 
 print " 15.1 Solicitud de Baja del msisdn $T_Msisdn\n";
 if ($T_Id_Submvno ne "CABLEMOVIL")
   {
     print " 15.2 Baja del cliente $T_Id_Cliente\n";
     print " 15.3 Baja por solicitud del cliente $T_Id_Cliente y todos sus msisdn\n";
     print " 15.4 Baja del cliente $T_Id_Cliente y todos sus msisdn por IMPAGO\n";
	 print " 15.6 Reactivar cliente $T_Id_Cliente dado de baja\n";
   }
   else
   {
     print " Aviso: linea $T_Id_Submvno. No podemos dar de baja clientes, ya que pueden tener productos en telefonia fija.\n";
   }
 print "   99 Menu Principal\n";

 $opcion2= prompt("Dime que quieres hacer");

 if ($opcion2 eq "15.1") {print "\n";
                        $qry_baja_port=qq{SELECT operadorReceptor,fechaSolicitud,fechaPortabilidad,MSISDN,estado,subEstado,fecha,hora 
                                                FROM  pv_solicitudes_baja_portabilidad WHERE  MSISDN='$T_Msisdn' and fechaPortabilidad>='$fecha' order by fechaPortabilidad};
                        $st_baja_port=$db_mnet->prepare($qry_baja_port);
                        $st_baja_port->execute();
                        my($bp_count)=0;
                        print "operadorReceptor,fechaSolicitud,fechaPortabilidad,MSISDN,estado,subEstado,fecha,hora\n";
                        while (($T_bp_operadorReceptor,$T_bp_fechaSolicitud,$T_bp_fechaPortabilidad,$T_bp_MSISDN,$T_bp_estado,$T_bp_subEstado,$T_bp_fecha,$T_bp_hora)=$st_baja_port->fetchrow_array() )
                         {
                                  print "$T_bp_operadorReceptor,$T_bp_fechaSolicitud,$T_bp_fechaPortabilidad,$T_bp_MSISDN,$T_bp_estado,$T_bp_subEstado,$T_bp_fecha,$T_bp_hora\n";
                                $bp_count++;
                         }
                         $st_baja_port->finish();
                        if($bp_count gt 0){
                                print "\n***ATENCION la linea no puede darse de baja, esta inmersa en un proceso de portabilidad***\n";
                                print "********************************************************************************************\n";
                        }



                        &consulta("mvno","SELECT * FROM pv_iccid_estados ORDER BY Id_Estado","pv_iccid_estados","G");


                        $motivo_baja=prompt("Dime el codigo de motivo de la baja");
                          $descripcion=prompt("Dime el motivo de la baja");
                          print "ATENCION!!!! Para cancelar la baja, marcar como estado X: en tabla mnet.pv_mvno_solicitudes_baja_msisdn\n";
                          print "Motivo baja: $motivo_baja - $descripcion \n";
                          $confirmacion=prompt("Confirmas baja por $motivo_baja (s/n)");

                          if ($confirmacion eq "s")
                            {
                               ### $respuesta=&msisdnCambioEstadoRed($T_Msisdn,$T_Id_Prepago_Mvno,$N_Numeracion_Reciclada,$T_Id_Cliente, $T_Id_Cliente_Fact,"T","miramsisdn","B","Solicitud miramsisdn $motivo_baja");

                               $respuesta=&msisdnSolicitudBajaMsisdn($T_Msisdn,$T_Id_Prepago_Mvno,$N_Numeracion_Reciclada,$T_Id_Cliente, $T_Id_Cliente_Fact,"T","miramsisdn",$motivo_baja,"Solicitud miramsisdn $descripcion");

				print $respuesta."\n";

                                &consulta("mnet","SELECT * FROM pv_mvno_solicitudes_baja_msisdn WHERE Msisdn='$T_Msisdn'","pv_mvno_solicitudes_baja_msisdn","G");

				#if($T_Id_Prepago_Mvno eq "T")
                                #{
                                #$qry_activa=qq{INSERT INTO pt_msisdn_actualiza_postpago SET MSISDN='$T_Msisdn', Id_Accion='B', Id_Motivo='$motivo_baja'
                                #               ON DUPLICATE KEY UPDATE MSISDN='$T_Msisdn', Id_Accion='B', Id_Motivo='$motivo_baja'};
                                #}
                                #else
                                #{
                                #$qry_activa=qq{INSERT INTO pt_msisdn_actualiza SET MSISDN='$T_Msisdn', Id_Accion='B', Id_Motivo='$motivo_baja'
                                #               ON DUPLICATE KEY UPDATE MSISDN='$T_Msisdn', Id_Accion='B', Id_Motivo='$motivo_baja'};
                                #}

                             #$st_activa=$db_mvno->prepare($qry_activa);
                             #$st_activa->execute();
                             #$st_activa->finish();
                             #print $qry_activa."\n";
                             ### &guardalogmsisdn($T_Msisdn,$T_Id_Cliente,$T_Id_Cliente_Fact,"CicloVida","Baja",$descripcion);
                             ### &baja_bonos($T_Msisdn,$T_Id_Cliente,$T_Id_Cliente_Fact,$descripcion,"T","miramsisdn");

                            }

                          $opcion2="99"
                        }

  if ($opcion2 eq "15.2") {print "\n";
                        if($T_Id_Cliente eq ''){
                                $T_Id_Cliente =prompt("Dime numero cliente:");
                        }
                                                print "Motivo de baja para el cliente:\n\n";
                                                print " 1. Baja solicitada por cliente\n";
                                                print " 2. Baja solicitada por Impago\n";
                                                print "   99 Salir\n\n";

                                                $opcionBaja="";
                        $opcionBaja=prompt("Elije la opcion de baja\n");
                        $bajaDescrip="";
                        if($opcionBaja ne "99"){
                                        if($opcionBaja eq "1"){
                                                $bajaDescrip="Baja solicitada por cliente ";
                                                $Id_Departamento="MVNO-CALLCENTER";
                                        }
                                        if($opcionBaja eq "2"){
                                                $bajaDescrip="Baja solicitada por Impago ";
                                                $Id_Departamento="FINANCIERO";
                                        }
                                        $motivo_baja=prompt("Dime el motivo:\n");
                                        $confirmacion=prompt("Confirmas $bajaDescrip $motivo_baja (s/n)");
                                }

                        if ($confirmacion eq "s"){
                                $qry_activa=qq{UPDATE xprv.pv_clientes SET Id_Estado_Cliente='400',
                                                                        Id_Sub_Estado_Cliente='00',
                                                                        Fecha_Id_Estado='$fecha' 
                                                                        WHERE Id_Cliente='$T_Id_Cliente'};
                                        $st_activa=$db_xprv->prepare($qry_activa);
                                $st_activa->execute();
                                $st_activa->finish();
                                print $qry_activa."\n";

                                 $qry_nota=qq{INSERT INTO xprv.pv_clientes_notas SET Id_Cliente='$T_Id_Cliente',
                                                                Fecha_Nota='$fecha',
                                                                Hora_Nota='$hora',
                                                                Id_Departamento='$Id_Departamento',
                                                                Id_Usuario='miramsisdn',
                                                                Descripcion='$bajaDescrip $motivo_baja'};

                                     $st_nota=$db_xprv->prepare($qry_nota);
                                     $st_nota->execute();
                                     $st_nota->finish();
                                     print $qry_nota."\n";
                                }
                          $opcion2="99"
                         }


if ($opcion2 eq "15.3") {

                        $tt=prompt("Dime el TT que indica dar de baja el cliente $T_Id_Cliente por SOLICITUD CLIENTE:\n");
                        print "MUY IMPORTANTE. DAREMOS DE BAJA TODAS LAS LINEAS DEL CLIENTE del MVNO!!!!\n";
                        $id_usuario="root";
                        $id_usuario=prompt("Id_Usuario con el que reescalar el TT?",$id_usuario);

                        $qry_tt=qq{SELECT Id_Usuario_Origen, Id_Dep_Origen, Id_ClienteCarrier, Id_Problema_Detalle FROM inf_ttaverias WHERE Id_TtAveria='$tt'};
                        $st_tt=$db_xtts->prepare($qry_tt);
                        $st_tt->execute();
                        ($T_Id_Usuario_Origen, $T_Id_Dep_Origen, $T_Id_ClienteCarrier, $T_Id_Problema_Detalle)=$st_tt->fetchrow_array();
                        $st_tt->finish();

                        print "\nEl TT $tt indica: $T_Id_Problema_Detalle\n\n";

                        $confirmacion=prompt("Confirmas la baja por solicitud del cliente $T_Id_Cliente y reescalado a $T_Id_Dep_Origen? (s/n)");

                        if ($T_Id_ClienteCarrier ne $T_Id_Cliente)
                          {
                           print "ERROR!!!! EL TT esta bajo el cliente $T_Id_ClienteCarrier y estas consultando el cliente $T_Id_Cliente\n\n";
                           $confirmacion="ERROR";
                          }



                        if ($confirmacion eq "s"){
                                $qry_activa=qq{UPDATE xprv.pv_clientes SET Id_Estado_Cliente='400',
                                                                        Id_Sub_Estado_Cliente='08',
                                                                        Fecha_Id_Estado='$fecha' 
                                                                        WHERE Id_Cliente='$T_Id_Cliente'};
                                $st_activa=$db_xprv->prepare($qry_activa);
                                $st_activa->execute();
                                $st_activa->finish();
                                print $qry_activa."\n";

                                $motivo_baja="Baja por peticion del cliente. Solicitud TT-".$tt.".";

                                   $qry_nota=qq{INSERT INTO xprv.pv_clientes_notas SET 
                                                                   Id_Cliente      ='$T_Id_Cliente',
                                                                   Fecha_Nota      ='$fecha',
                                                                   Hora_Nota       ='$hora',
                                                                   Id_Departamento ='$T_Id_Dep_Origen',
                                                                   Id_Usuario      ='$T_Id_Usuario_Origen',
                                                                   Descripcion     ='$motivo_baja'};
                                   $st_nota=$db_xprv->prepare($qry_nota);
                                   $st_nota->execute();
                                   $st_nota->finish();
                                   print $qry_nota."\n";

                                   $qry_msisdn_baja=qq{SELECT Msisdn, Id_Cliente, Id_Cliente_Fact, Id_Prepago_Mvno FROM pv_msisdn 
                                                   WHERE Id_Cliente='$T_Id_Cliente' AND Id_Estado_Actual<>'B'};
                                   $st_msisdn_baja=$db_mvno->prepare($qry_msisdn_baja);
                                   $st_msisdn_baja->execute();

                                   while ( ($msisdn_baja, $id_cliente_baja, $id_cliente_fact_baja, $id_prepago_baja)=$st_msisdn_baja->fetchrow_array() )
                                        {
                                       print "$msisdn_baja, $id_cliente_baja, $id_cliente_fact_baja, $id_prepago_baja\n";
                                       $qry_baja_port=qq{SELECT operadorReceptor,fechaSolicitud,fechaPortabilidad,
                                                                MSISDN,estado,subEstado,fecha,hora 
                                                           FROM pv_solicitudes_baja_portabilidad 
                                                          WHERE MSISDN='$msisdn_baja' and fechaPortabilidad>='$fecha' order by fechaPortabilidad};
                                       $st_baja_port=$db_mnet->prepare($qry_baja_port);
                                       $st_baja_port->execute();
                                       my($bp_count)=0;
                                       print "operadorReceptor,fechaSolicitud,fechaPortabilidad,MSISDN,estado,subEstado,fecha,hora\n";
                                       while (($T_bp_operadorReceptor,$T_bp_fechaSolicitud,$T_bp_fechaPortabilidad,$T_bp_MSISDN,
                                               $T_bp_estado,$T_bp_subEstado,$T_bp_fecha,$T_bp_hora)=$st_baja_port->fetchrow_array() )
                                            {
                                              print "$T_bp_operadorReceptor,$T_bp_fechaSolicitud,$T_bp_fechaPortabilidad,$T_bp_MSISDN,";
                                              print "$T_bp_estado,$T_bp_subEstado,$T_bp_fecha,$T_bp_hora\n";
                                              $bp_count++;
                                            }
                                       $st_baja_port->finish();

#$bp_count=0;
                                       if($bp_count gt 0){
                                         print "\n***ATENCION la linea no puede darse de baja, esta inmersa en un proceso de portabilidad***\n";
                                         print "********************************************************************************************\n";
                                        }
                                        else
                                        {
                              
                                          $motivo_baja="480"; 
                                          $respuesta=&msisdnSolicitudBajaMsisdn($msisdn_baja,$T_Id_Prepago_Mvno,$N_Numeracion_Reciclada,$T_Id_Cliente, $T_Id_Cliente_Fact,"T","miramsisdn",$motivo_baja,"Solicitud miramsisdn $descripcion");

                                         # &guardalogmsisdn($msisdn_baja,$id_cliente_baja,$id_cliente_fact_baja,"CicloVida","Baja",$motivo_baja);
                                         # &baja_bonos($msisdn_baja,$id_cliente_baja,$id_cliente_fact_baja,$motivo_baja,"X",$T_Id_Usuario_Origen);
                                        }

                                     }
                                $st_msisdn_baja->finish();

                                $hora_tt=substr($hora,0,2).":".substr($hora,2,2);

                                $qry_update_tt=qq{UPDATE inf_ttaverias 
                                                     SET Ultima_Modificacion_Fecha      ='$fecha',
                                                         Ultima_Modificacion_Hora       ='$hora_tt',
                                                         Ultima_Modificacion_Id_Usuario ='$id_usuario', 
                                                         Ultima_Modificacion_Id_Dep     ='MVNO-INCIDENCIAS',
                                                         Id_Usuario_Actual              ='',
                                                         Id_Dep_Actual                  ='$T_Id_Dep_Origen'
                                                   WHERE Id_TtAveria='$tt'};
                                $st_update_tt=$db_xtts->prepare($qry_update_tt);
                                $st_update_tt->execute();
                                $st_update_tt->finish();

                                print $qry_update_tt."\n";

                                $qry_mete_tt=qq{INSERT INTO inf_ttaverias_actua SET
                                                          Id_TtAveria     ='$tt', 
                                                          Fecha_Actuacion ='$fecha',
                                                          Hora_Actuacion  ='$hora_tt',
                                                          Id_Usuario      ='$id_usuario',
                                                          Id_Departamento ='MVNO-INCIDENCIAS',
                                                          Descripcion     ='Baja por solicitud del cliente $T_Id_Cliente realizada. Linea/s pasa a estado J/S. Trancurridos 7 dias se procedera a la baja definitiva e irrecuperable de linea/s',
                                                          Id_Escalado     ='E',
                                                          Id_Dep_Escalado ='$T_Id_Dep_Origen',
                                                          Flag_Notificado ='X'};
                               $st_mete_tt=$db_xtts->prepare($qry_mete_tt);
                               $st_mete_tt->execute();
                               $st_mete_tt->finish();

                               print $qry_mete_tt."\n";


                               } # if ($confirmacion eq "s")

                          $opcion2="99"
                        }

if ($opcion2 eq "15.4") {

                        $tt=prompt("Dime el TT que indica dar de baja el cliente $T_Id_Cliente por IMPAGO:\n");
                        print "MUY IMPORTANTE. DAREMOS DE BAJA TODAS LAS LINEAS DEL CLIENTE del MVNO!!!!\n";
                        $id_usuario="root";
                        $id_usuario=prompt("Id_Usuario con el que reescalar el TT?",$id_usuario);

                        $qry_tt=qq{SELECT Id_Usuario_Origen, Id_Dep_Origen, Id_ClienteCarrier, Id_Problema_Detalle FROM inf_ttaverias WHERE Id_TtAveria='$tt'};
                        $st_tt=$db_xtts->prepare($qry_tt);
                        $st_tt->execute();
                        ($T_Id_Usuario_Origen, $T_Id_Dep_Origen, $T_Id_ClienteCarrier, $T_Id_Problema_Detalle)=$st_tt->fetchrow_array();
                        $st_tt->finish();

                        print "\nEl TT $tt indica: $T_Id_Problema_Detalle\n\n";

                        $confirmacion=prompt("Confirmas la baja por IMPAGO del cliente $T_Id_Cliente y reescalado a $T_Id_Dep_Origen? (s/n)");

                        if ($T_Id_ClienteCarrier ne $T_Id_Cliente)
                          {
                           print "ERROR!!!! EL TT esta bajo el cliente $T_Id_ClienteCarrier y estas consultando el cliente $T_Id_Cliente\n\n";
                           $confirmacion="ERROR";
                          }



                        if ($confirmacion eq "s"){
                                $qry_activa=qq{UPDATE xprv.pv_clientes SET Id_Estado_Cliente='400',
                                                                        Id_Sub_Estado_Cliente='06',
                                                                        Fecha_Id_Estado='$fecha' 
                                                                        WHERE Id_Cliente='$T_Id_Cliente'};
                                $st_activa=$db_xprv->prepare($qry_activa);
                                $st_activa->execute();
                                $st_activa->finish();
                                print $qry_activa."\n";

                                $motivo_baja="Baja cliente por impago. Solicitud TT-".$tt.".";

                                   $qry_nota=qq{INSERT INTO xprv.pv_clientes_notas SET 
                                                                   Id_Cliente      ='$T_Id_Cliente',
                                                                   Fecha_Nota      ='$fecha',
                                                                   Hora_Nota       ='$hora',
                                                                   Id_Departamento ='$T_Id_Dep_Origen',
                                                                   Id_Usuario      ='$T_Id_Usuario_Origen',
                                                                   Descripcion     ='$motivo_baja'};
                                   $st_nota=$db_xprv->prepare($qry_nota);
                                   $st_nota->execute();
                                   $st_nota->finish();
                                   print $qry_nota."\n";

                                   $qry_msisdn_baja=qq{SELECT Msisdn, Id_Cliente, Id_Cliente_Fact, Id_Prepago_Mvno FROM pv_msisdn 
                                                   WHERE Id_Cliente='$T_Id_Cliente' AND Id_Estado_Actual<>'B'};
                                   $st_msisdn_baja=$db_mvno->prepare($qry_msisdn_baja);
                                   $st_msisdn_baja->execute();

                                   while ( ($msisdn_baja, $id_cliente_baja, $id_cliente_fact_baja, $id_prepago_baja)=$st_msisdn_baja->fetchrow_array() )
                                        {
                                       print "$msisdn_baja, $id_cliente_baja, $id_cliente_fact_baja, $id_prepago_baja\n";
                                       $qry_baja_port=qq{SELECT operadorReceptor,fechaSolicitud,fechaPortabilidad,
                                                                MSISDN,estado,subEstado,fecha,hora 
                                                           FROM pv_solicitudes_baja_portabilidad 
                                                          WHERE MSISDN='$msisdn_baja' and fechaPortabilidad>='$fecha' order by fechaPortabilidad};
                                       $st_baja_port=$db_mnet->prepare($qry_baja_port);
                                       $st_baja_port->execute();
                                       my($bp_count)=0;
                                       print "operadorReceptor,fechaSolicitud,fechaPortabilidad,MSISDN,estado,subEstado,fecha,hora\n";
                                       while (($T_bp_operadorReceptor,$T_bp_fechaSolicitud,$T_bp_fechaPortabilidad,$T_bp_MSISDN,
                                               $T_bp_estado,$T_bp_subEstado,$T_bp_fecha,$T_bp_hora)=$st_baja_port->fetchrow_array() )
                                            {
                                              print "$T_bp_operadorReceptor,$T_bp_fechaSolicitud,$T_bp_fechaPortabilidad,$T_bp_MSISDN,";
                                              print "$T_bp_estado,$T_bp_subEstado,$T_bp_fecha,$T_bp_hora\n";
                                              $bp_count++;
                                            }
                                       $st_baja_port->finish();

                                       if($bp_count gt 0){
                                         print "\n***ATENCION la linea no puede darse de baja, esta inmersa en un proceso de portabilidad***\n";
                                         print "********************************************************************************************\n";
                                        }
                                        else
                                        {

                                          $motivo_baja="810";
                                          $respuesta=&msisdnSolicitudBajaMsisdn($msisdn_baja,$T_Id_Prepago_Mvno,$N_Numeracion_Reciclada,$T_Id_Cliente, $T_Id_Cliente_Fact,"T","miramsisdn",$motivo_baja,"Solicitud miramsisdn $descripcion");

                                        #  &guardalogmsisdn($msisdn_baja,$id_cliente_baja,$id_cliente_fact_baja,"CicloVida","Baja",$motivo_baja);
                                        #  &baja_bonos($msisdn_baja,$id_cliente_baja,$id_cliente_fact_baja,$motivo_baja,"X",$T_Id_Usuario_Origen);
                                        }

                                     }
                                $st_msisdn_baja->finish();

                                $hora_tt=substr($hora,0,2).":".substr($hora,2,2);

                                $qry_update_tt=qq{UPDATE inf_ttaverias 
                                                     SET Ultima_Modificacion_Fecha      ='$fecha',
                                                         Ultima_Modificacion_Hora       ='$hora_tt',
                                                         Ultima_Modificacion_Id_Usuario ='$id_usuario', 
                                                         Ultima_Modificacion_Id_Dep     ='MVNO-INCIDENCIAS',
                                                         Id_Usuario_Actual              ='',
                                                         Id_Dep_Actual                  ='$T_Id_Dep_Origen'
                                                   WHERE Id_TtAveria='$tt'};
                                $st_update_tt=$db_xtts->prepare($qry_update_tt);
                                $st_update_tt->execute();
                                $st_update_tt->finish();

                                print $qry_update_tt."\n";

                                $qry_mete_tt=qq{INSERT INTO inf_ttaverias_actua SET
                                                          Id_TtAveria     ='$tt', 
                                                          Fecha_Actuacion ='$fecha',
                                                          Hora_Actuacion  ='$hora_tt',
                                                          Id_Usuario      ='$id_usuario',
                                                          Id_Departamento ='MVNO-INCIDENCIAS',
                                                          Descripcion     ='Baja por impago del cliente $T_Id_Cliente realizada.Linea/s pasa a estado J/S. Trancurridos 7 dias se procedera a la baja definitiva e irrecuperable de linea/s',
                                                          Id_Escalado     ='E',
                                                          Id_Dep_Escalado ='$T_Id_Dep_Origen',
                                                          Flag_Notificado ='X'};
                               $st_mete_tt=$db_xtts->prepare($qry_mete_tt);
                               $st_mete_tt->execute();
                               $st_mete_tt->finish();

                               print $qry_mete_tt."\n";


                               } # if ($confirmacion eq "s")

                          $opcion2="99"
                        }#15.4
						
						if ($opcion2 eq "15.6") {print "\n";
                        if($T_Id_Cliente eq ''){
                                $T_Id_Cliente =prompt("Dime numero cliente:");
                        }
                             
                        $bajaDescrip="Activar de nuevo al cliente";
                        $motivo_baja=prompt("Dime el motivo:\n");
                        $confirmacion=prompt("Confirmas $bajaDescrip $motivo_baja (s/n)");
                                

                        if ($confirmacion eq "s"){
                                $qry_activa=qq{UPDATE xprv.pv_clientes SET Id_Estado_Cliente='000',
                                                                        Id_Sub_Estado_Cliente='00',
                                                                        Fecha_Id_Estado='$fecha' 
                                                                        WHERE Id_Cliente='$T_Id_Cliente'};
                                        $st_activa=$db_xprv->prepare($qry_activa);
                                $st_activa->execute();
                                $st_activa->finish();
                                print $qry_activa."\n";

                                 $qry_nota=qq{INSERT INTO xprv.pv_clientes_notas SET Id_Cliente='$T_Id_Cliente',
                                                                Fecha_Nota='$fecha',
                                                                Hora_Nota='$hora',
                                                                Id_Departamento='$Id_Departamento',
                                                                Id_Usuario='miramsisdn',
                                                                Descripcion='$bajaDescrip $motivo_baja'};

                                     $st_nota=$db_xprv->prepare($qry_nota);
                                     $st_nota->execute();
                                     $st_nota->finish();
                                     print $qry_nota."\n";
                                }
                          $opcion2="99"
                         }







 } # while ($opcion2 ne "99")

} # bajas












sub logs

{

 my($T_Msisdn, $T_Id_Cliente)=@_;


 $opcion2="";

 $fecha_log=$fecha;

while ($opcion2 ne "99")

 {
 print "-------------------------------\n";
 print "| 90. Logs del sistema        |\n";
 print "-------------------------------\n";
 print "  Msisdn: $T_Msisdn Prepago: $T_Id_Prepago ($T_Id_Prepago_Mvno)\n";
 print " 90.1 Consulta todos los logs de /var/log/thor del msisdn por fecha\n";
 print "\n";

 $opcion2= prompt("Dime que quieres hacer");

 

 if ($opcion2 eq "90.1") {$fecha_log=prompt("Fecha a consultar",$fecha_log);
                          $comando="cat /var/log/thor/".$fecha_log."* | grep '".$T_Msisdn."'";
                          @log=`$comando`;
                          foreach $line(@log) { print $line;}
                         }

 }
 return(0);
} # logs





sub buclecdrs


{

($desde_fecha,$hasta_fecha)=@_;

my(@fechas)=();


$st_tablas = $db_xcdr->prepare ("SHOW TABLES");
$st_tablas -> execute();

while (($que_tabla)=$st_tablas->fetchrow_array())

{

 if ( ($que_tabla ge $desdetabla) && ($que_tabla le $hastatabla) )

    {
      if (length($que_tabla)>16)
      {
       $que_fecha=substr($que_tabla,10,8);
       print ("Comando: ".$queComando." ".$que_fecha."\n");
       @logcomando=`$queComando $que_fecha`;
       foreach $line(@logcomando)
       {
        print $line;
       }
     }

    }

return(@fechas);

}

$st_tablas -> finish();






}




sub consulta

{

 my ($database, $query, $descripcion, $resultado)=@_;




 if ($resultado eq "G")
   {
    @datos=`mysql $database -e "$query"`;
    foreach $line(@datos)
      {
       print $line;
      }
   }


 if ($resultado eq "L")
   {

 if ($database eq "mvno") {$st_consulta=$db_mvno->prepare($query);}
 if ($database eq "mlog") {$st_consulta=$db_mlog->prepare($query);}
 if ($database eq "mnet") {$st_consulta=$db_mnet->prepare($query);}
 if ($database eq "mcdf") {$st_consulta=$db_mcdf->prepare($query);}
 if ($database eq "mcdr") {$st_consulta=$db_mcdr->prepare($query);}
 if ($database eq "mcdi") {$st_consulta=$db_mcdi->prepare($query);}
 if ($database eq "mcdc") {$st_consulta=$db_mcdc->prepare($query);}
 if ($database eq "xprv") {$st_consulta=$db_xprv->prepare($query);}
 if ($database eq "xtts") {$st_consulta=$db_xtts->prepare($query);}
 if ($database eq "mopb") {$st_consulta=$db_mopb->prepare($query);}

 print $query."\n";



 $st_consulta->execute();

 $cabecera=$st_consulta->{'NAME'};
 $campos = $st_consulta->{'NUM_OF_FIELDS'};

 print "***".$descripcion."***\n";

 if ($resultado eq "L")
  {
    while (@datos=$st_consulta->fetchrow_array() )
      {
       for ($n=0; $n<$campos; $n++)
         {
          $$cabecera[$n]=$$cabecera[$n]."                                        ";
          $$cabecera[$n]=substr($$cabecera[$n],0,35);

          print $$cabecera[$n]."|".@datos[$n]."\n";
         }
       print "------------------------------------------------------------------------------------\n";
       }
  }

if ($resultado eq "G")
  {
       for ($n=0; $n<$campos; $n++)
         {
          print $$cabecera[$n]."\t";
         }
       print "\n";

    while (@datos=$st_consulta->fetchrow_array() )
      {
       for ($n=0; $n<$campos; $n++)
         {
          print @datos[$n]."\t";
         }
         print "\n";
       }
    print "----------------------------------------------------------------------------------------\n";
  }

$st_consulta->finish();

 print "\n";

}

}





sub guardalogmsisdn

{

 my($er_Msisdn,$er_Id_Cliente,$er_Id_Cliente_Fact,$er_Id_Evento,$er_Id_Subevento,$er_Descripcion)=@_;

 $fecha=`/bin/date +"%Y%m%d"`;
 chomp($fecha);
 $hora=`/bin/date +"%H%M%S"`;
 chomp($hora);
 

               $qry_log_msisdn=qq{INSERT INTO log_msisdn SET
                                Msisdn          ='$er_Msisdn',
                                Id_Cliente      ='$er_Id_Cliente',
                                Id_Cliente_Fact ='$er_Id_Cliente_Fact',
                                Tipo_Usuario    ='T',
                                Id_Usuario      ='admin',
                                Fecha           ='$fecha',
                                Hora            ='$hora',
                                Id_Evento       ='$er_Id_Evento',
                                Id_Subevento    ='$er_Id_Subevento',
                                Descripcion     ='$er_Descripcion' };
               $st_log_msisdn=$db_mlog->prepare($qry_log_msisdn);
               $st_log_msisdn->execute();
               $st_log_msisdn->finish();

}





sub baja_bonos

{
 my($msisdn,$id_cliente,$id_cliente_fact,$descripcion)=@_;

 
 $qry_bonos=qq{SELECT Id_Bono FROM pv_mvno_bonos_msisdn Where Msisdn='$msisdn' AND Id_Cliente='$id_cliente'};
 $st_bonos=$db_mvno->prepare($qry_bonos);
 $st_bonos->execute();

 print $qry_bonos."\n";

 while ( ($T_Id_Bono)=$st_bonos->fetchrow_array() )
   {
    $qry_bono_baja=qq{INSERT INTO pv_solicitudes_bonos SET
                        Tipo_Solicitud   ='B',
			Fecha            ='$fecha',
			Hora             ='$hora',
			Tipo_Usuario     ='T',
			Id_Usuario       ='admin',
			Msisdn           ='$msisdn',
			Id_Cliente       ='$id_cliente',
			Id_Cliente_Fact  ='$id_cliente_fact',
			Fecha_Realizacion='$fecha',
			Id_Bono          ='$T_Id_Bono',
			Observaciones    ='$descripcion',
                        Flag_Realizado   ='N'};
     print $qry_bono_baja."\n";

     $st_bono_baja=$db_mnet->prepare($qry_bono_baja);
     $st_bono_baja->execute();
     $st_bono_baja->finish();
   }

 $st_bonos->finish();




}
