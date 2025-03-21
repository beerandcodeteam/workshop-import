<x-layout>
    <div class="px-4 sm:px-6 lg:px-8">
        <div class="sm:flex sm:items-center">
            <div class="sm:flex-auto">
                <h1 class="text-base font-semibold text-gray-900">Clientes</h1>
                <p class="mt-2 text-sm text-gray-700">Lista de todos os clientes, suas propriedades e conjuges</p>
            </div>

            <div x-data>
                <form method="POST" action="{{ route('customer.import') }}" enctype="multipart/form-data" x-ref="uploadForm">
                    @csrf
                    <input
                        type="file"
                        name="customers_file"
                        class="hidden"
                        x-ref="fileInput"
                        @change="$refs.uploadForm.submit()"
                    >
                    <button
                        type="button"
                        @click="$refs.fileInput.click()"
                        class="block rounded-md bg-indigo-600 px-3 py-2 text-center text-sm font-semibold text-white shadow-xs hover:bg-indigo-500 focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-indigo-600"
                    >
                        Add user
                    </button>
                </form>
            </div>
        </div>

        @session('message')
            <div class="bg-green-200 flex flex-row w-full rounded-lg p-4 text-green-900 border-green-900 mt-4 ">
                O Arquivo esta sendo processado
            </div>
        @endsession

        <div x-data="logFileBroadcast()" x-init="init()" class="flex flex-row w-full">
            <div x-show="url" class="bg-red-200 flex flex-row w-full rounded-lg p-4 text-red-900 border-red-900 mt-4">
                O arquivo tem problemas na importação: <a class="ml-4 font-bold underline" :href="url" target="_blank">Ver problemas</a>
            </div>
        </div>

        <div x-data="logFileBroadcast()" x-init="init()" class="flex flex-row w-full">
            <div x-show="completed" class="bg-green-200 flex flex-row w-full rounded-lg p-4 text-green-900 border-green-900 mt-4">
                Importação concluida com sucesso
            </div>
        </div>
        <div class="mt-8 flow-root">
            <div class="-mx-4 -my-2 overflow-x-auto sm:-mx-6 lg:-mx-8">
                <div class="inline-block min-w-full py-2 align-middle sm:px-6 lg:px-8">
                    <div class="overflow-hidden ring-1 shadow-sm ring-black/5 sm:rounded-lg">
                        <table class="min-w-full divide-y divide-gray-300">
                            <thead class="bg-gray-50">
                            <tr>
                                <th scope="col" class="py-3.5 pr-3 pl-4 text-left text-sm font-semibold text-gray-900 sm:pl-6">Unidade</th>
                                <th scope="col" class="py-3.5 pr-3 pl-4 text-left text-sm font-semibold text-gray-900 sm:pl-6">Bloco</th>
                                <th scope="col" class="py-3.5 pr-3 pl-4 text-left text-sm font-semibold text-gray-900 sm:pl-6">Valor de compra</th>
                                <th scope="col" class="py-3.5 pr-3 pl-4 text-left text-sm font-semibold text-gray-900 sm:pl-6">Saldo devedor</th>
                                <th scope="col" class="py-3.5 pr-3 pl-4 text-left text-sm font-semibold text-gray-900 sm:pl-6">Nome Titular</th>
                                <th scope="col" class="px-3 py-3.5 text-left text-sm font-semibold text-gray-900">CPF</th>
                                <th scope="col" class="px-3 py-3.5 text-left text-sm font-semibold text-gray-900">RG</th>
                                <th scope="col" class="px-3 py-3.5 text-left text-sm font-semibold text-gray-900">RG EMISSOR</th>
                                <th scope="col" class="px-3 py-3.5 text-left text-sm font-semibold text-gray-900">RG Data de Emissao</th>
                                <th scope="col" class="px-3 py-3.5 text-left text-sm font-semibold text-gray-900">Telefone</th>
                                <th scope="col" class="py-3.5 pr-3 pl-4 text-left text-sm font-semibold text-gray-900 sm:pl-6">Conjuge</th>
                                <th scope="col" class="py-3.5 pr-3 pl-4 text-left text-sm font-semibold text-gray-900 sm:pl-6">CPF Conjuge</th>
                                <th scope="col" class="py-3.5 pr-3 pl-4 text-left text-sm font-semibold text-gray-900 sm:pl-6">Telefone Conjuge</th>
                                <th scope="col" class="px-3 py-3.5 text-left text-sm font-semibold text-gray-900">CEP</th>
                                <th scope="col" class="px-3 py-3.5 text-left text-sm font-semibold text-gray-900">Endereco</th>
                                <th scope="col" class="px-3 py-3.5 text-left text-sm font-semibold text-gray-900">Numero</th>
                                <th scope="col" class="px-3 py-3.5 text-left text-sm font-semibold text-gray-900">Bairro</th>
                                <th scope="col" class="px-3 py-3.5 text-left text-sm font-semibold text-gray-900">Cidade</th>
                                <th scope="col" class="px-3 py-3.5 text-left text-sm font-semibold text-gray-900">Estado</th>
                                <th scope="col" class="relative py-3.5 pr-4 pl-3 sm:pr-6">
                                    <span class="sr-only">Edit</span>
                                </th>
                            </tr>
                            </thead>
                            <tbody class="divide-y divide-gray-200 bg-white">
                            @foreach($customers as $customer)
                                <tr>
                                    <td class="py-4 pr-3 pl-4 text-sm font-medium whitespace-nowrap text-gray-900 sm:pl-6">{{ $customer->properties[0]->unit }}</td>
                                    <td class="px-3 py-4 text-sm whitespace-nowrap text-gray-500">{{ $customer->properties[0]->block }}</td>
                                    <td class="px-3 py-4 text-sm whitespace-nowrap text-gray-500">{{ $customer->properties[0]->buy_value }}</td>
                                    <td class="px-3 py-4 text-sm whitespace-nowrap text-gray-500">{{ $customer->properties[0]->outstanding_balance }}</td>
                                    <td class="px-3 py-4 text-sm whitespace-nowrap text-gray-500">{{ $customer->people[0]->name }}</td>
                                    <td class="px-3 py-4 text-sm whitespace-nowrap text-gray-500">{{ $customer->people[0]->cpf }}</td>
                                    <td class="px-3 py-4 text-sm whitespace-nowrap text-gray-500">{{ $customer->people[0]->rg }}</td>
                                    <td class="px-3 py-4 text-sm whitespace-nowrap text-gray-500">{{ $customer->people[0]->rg_emitter }}</td>
                                    <td class="px-3 py-4 text-sm whitespace-nowrap text-gray-500">{{ $customer->people[0]->rg_issue_date }}</td>
                                    <td class="px-3 py-4 text-sm whitespace-nowrap text-gray-500">{{ $customer->people[0]->phone }}</td>
                                    <td class="px-3 py-4 text-sm whitespace-nowrap text-gray-500">{{ $customer->people[1]->name }}</td>
                                    <td class="px-3 py-4 text-sm whitespace-nowrap text-gray-500">{{ $customer->people[1]->cpf }}</td>
                                    <td class="px-3 py-4 text-sm whitespace-nowrap text-gray-500">{{ $customer->people[1]->phone }}</td>
                                    <td class="px-3 py-4 text-sm whitespace-nowrap text-gray-500">{{ $customer->people[0]->addresses[0]->zipcode }}</td>
                                    <td class="px-3 py-4 text-sm whitespace-nowrap text-gray-500">{{ $customer->people[0]->addresses[0]->address }}</td>
                                    <td class="px-3 py-4 text-sm whitespace-nowrap text-gray-500">{{ $customer->people[0]->addresses[0]->number }}</td>
                                    <td class="px-3 py-4 text-sm whitespace-nowrap text-gray-500">{{ $customer->people[0]->addresses[0]->district }}</td>
                                    <td class="px-3 py-4 text-sm whitespace-nowrap text-gray-500">{{ $customer->people[0]->addresses[0]->city }}</td>
                                    <td class="px-3 py-4 text-sm whitespace-nowrap text-gray-500">{{ $customer->people[0]->addresses[0]->state }}</td>

                                    <td class="relative py-4 pr-4 pl-3 text-right text-sm font-medium whitespace-nowrap sm:pr-6">
                                        <a href="#" class="text-indigo-600 hover:text-indigo-900">Edit<span class="sr-only">, Lindsay Walton</span></a>
                                    </td>
                                </tr>
                            @endforeach
                            <!-- More people... -->
                            </tbody>
                        </table>


                    </div>


                </div>


            </div>
            <div class="mt-16">
                {{ $customers->links() }}
            </div>
        </div>
    </div>

    <script>
        function logFileBroadcast() {
            return {
                url: "",
                completed: false,
                init() {
                    console.log("INICIANDO")
                    // Subscrição ao canal público "public-channel"
                    Echo.channel('error-file-detected')
                        .listen('.log.file.detected', (event) => {
                            console.log(event);

                            this.url = event.file

                        });

                    Echo.channel('import-completed')
                        .listen('.import.completed', (event) => {

                            this.completed = true;

                        });
                }
            }
        }
    </script>

</x-layout>
