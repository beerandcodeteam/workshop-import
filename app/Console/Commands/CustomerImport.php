<?php

namespace App\Console\Commands;

use App\Enums\MaritalStatusEnum;
use App\Events\ErrorFileDetected;
use App\Models\Customer;
use App\Traits\ImportHelper;
use Carbon\Carbon;
use Illuminate\Console\Command;

class CustomerImport extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */

    use ImportHelper;
    protected $signature = 'app:customer-import';

    /**
     * The console command description.
     *
     * @var string
     */
    protected $description = 'Command description';

    /**
     * Execute the console command.
     */
    public function handleImport($filePath)
    {
        $this->import08Concurrent($filePath);
    }
}
