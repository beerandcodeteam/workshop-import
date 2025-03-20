<?php

use Illuminate\Support\Facades\Route;

Route::get('/', [\App\Http\Controllers\CustomerController::class, 'index']);
Route::post('/import', [\App\Http\Controllers\CustomerController::class, 'import'])->name('customer.import');
