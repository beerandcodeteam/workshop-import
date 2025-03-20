<?php

namespace App\Http\Controllers;

use App\Imports\CustomerImport;
use App\Jobs\ImportJob;
use App\Jobs\ValidateImportJob;
use App\Models\Customer;
use App\Traits\ImportHelper;
use Illuminate\Http\Request;
use Maatwebsite\Excel\Facades\Excel;

class CustomerController extends Controller
{

    use ImportHelper;
    public function index(Request $request)
    {
        return view('index', [
            'customers' => Customer::with('properties', 'people.spouse', 'people.addresses')->paginate(15)
        ]);
    }

    public function import(Request $request)
    {
        $file = $request->file('customers_file')->store('import', 'public');
        $file = \Storage::disk('public')->path($file);

        \Bus::chain([
            new ValidateImportJob($file),
            new ImportJob($file),
        ])->dispatch();

        return redirect()->back()->with('message', 'O esta sendo processado');
    }
}
